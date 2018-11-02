# -*- coding: utf-8 -*-
__author__ = 'Алпатов М.В.'
"""
Модуль полной выгрузки отчетности
"""
import datetime
import os
import re
import uuid
import hashlib
import regls
import workflow
from sbis import BLObject
from sbis import ComplectSwitcher
from sbis import Config
from sbis import CreateHstore
from sbis import CreateRecord
from sbis import CreateRecordFormat
from sbis import CreateRecordSet
from sbis import EndPoint
from sbis import Error
from sbis import FileTransfer
from sbis import LogMsg
from sbis import Navigation
from sbis import ParseHstore
from sbis import Person
from sbis import PrepareGetRPCInvocationURL
from sbis import Record
from sbis import RecordSet
from sbis import rk
from sbis import RpcFile
from sbis import Requirement
from sbis import Session
from sbis import SqlQuery
from sbis import SqlQueryScalar
from sbis import Warning
from sbis import WarningMsg
from sbis import Документ
from sbis import ДокументОтправка
from sbis import ОтчетныйПериод
from sbis import Событие
from sbis import ТипФормализованногоДокумента
from sbis import ЭЦП
from py.export.ExportHelpers import ExportHelpers, ExportZip

# Скоуп для Сервиса параметров, на уровне аккаунта
SCOPE_TYPE = 3

# Словарь из названий направления, типа документов и типа инспекций
TYPES_DICT = {
    'ОтчетФНС': ['ФНС', 'Отчетность', 1],
    'ОтчетПФР': ['ПФР', 'Отчетность', 2],
    'ОтчетРОССТАТ': ['РОССТАТ', 'Отчетность', 3],
    'ОтчетФСС': ['ФСС', 'Отчетность', 4],
    'ОтчетРПН': ['РПН', 'Отчетность', 7],
    'ОтчетФСРАР': ['ФСРАР', 'Отчетность', 9],
    'ОтчетУФМС': ['МВД', 'Отчетность', 10],
    'РеестрФСС': ['ФСС', 'Отчетность', 4],
    'ПенсияПФР': ['ПФР', 'Отчетность', 2],
    'РетроконверсияПФР': ['ПФР', 'Отчетность', 2],
    'ОтчетЦБ': ['ЦБ', 'Отчетность', 13],
    'ПредставлениеФНС': ['ФНС', 'ПредставлениеФНС', 1],
    'ИстребованиеФНС': ['ФНС', 'ИстребованиеФНС', 1],
    'ПисьмоФНС': ['ФНС', 'Письма', 1],
    'ПисьмоПФР': ['ПФР', 'Письма', 2],
    'ПисьмоРОССТАТ': ['РОССТАТ', 'Письма', 3],
    'ЗапросПФР': ['ПФР', 'Сверки', 2],
    'ЗапросФНС': ['ФНС', 'Сверки', 1],
    'ОтчетРоструд': ['ЦЗН', 'Отчетность', 14]
}

# Количество комплектов на одну задачу
ITEMS_FOR_TASK = Config.GetInt32(Config.Instance(), 'Электронная отчетность.Выгрузка.КоличествоКомплектовНаЗадачу', 15)

# Приоритет для сценария
WORKFLOW_PRIORITY = 3

# Лимит задач на одного пользователя
TASKS_LIMIT = 2

# Имя сервиса
SERVICE_NAME = 'Основной сервис'

# Выгрузка отчетности
TYPE_EXPORT_REPORTS = 0

# Выгрузка требований
TYPE_EXPORT_REQUIREMENTS = 1

# Значения для битовой маски. По маске определяем то, что хотим выгружать
# Отчеты
NEED_REPORTS = 1
# Письма(исходящие)
NEED_MAILS = 2
# Сверки
NEED_REVISE = 4
# Требования
NEED_REQUIRES = 8


def prepare_full_export(params):
    """
    Метод получает комплекты для выгрузки, формирует сценарий в LRS, запускает задачи
    """
    LogMsg('prepare_full_export: params: ' + str(params))
    reqs_list = []
    tasks_count_reports = 0
    tasks_count_reqs = 0
    filter_params = Record(params.as_dict())
    if params.Get('Scope') & NEED_REQUIRES:
        reqs_list = __get_requirements(params)
        # Для массовой выгрузки не спрашиваем у этого метода требования
        filter_params.Set('Scope', filter_params.Get('Scope') - NEED_REQUIRES)
    kit_list = __get_complects(filter_params, reqs_list)
    if not kit_list and not reqs_list:
        raise Warning('Нет комплектов', 'За указанный период комплектов не найдено', 1)
    LogMsg('kit_list: ' + str(kit_list))
    long_request_name = 'PrepareExport'
    show_result = True

    lrs_instance = workflow.LongRequestsService.Instance().CreateLongRequest(long_request_name,
                                                                             SERVICE_NAME,
                                                                             show_result)

    str_for_hash = params.AsJson(2)
    params_hash = hashlib.md5(str_for_hash.encode('utf-8')).hexdigest()
    merge_key = long_request_name + ':' + str(Session.ClientID()) + ':' + params_hash
    lrs_instance.SetMergePolicy(merge_key, workflow.MergePolicy.mpMERGE_ANY_STATE)
    lrs_instance.SetResponsible("Алпатов М.В.", "Комплекты отчетности")
    lrs_instance.SetDescription(rk("Выгрузка в Архив"))
    lrs_instance.SetPriority(WORKFLOW_PRIORITY)  # Приоритет сценария ниже среднего
    lrs_instance.OpenParallelBlock()
    if kit_list:
        # Количество параллельных задач для отчетности
        tasks_count_reports = len(kit_list) // ITEMS_FOR_TASK
        tasks_count_reports += 1 if len(kit_list) % ITEMS_FOR_TASK != 0 else 0
        for idx in range(tasks_count_reports):
            ids_for_task = kit_list[ITEMS_FOR_TASK * idx: ITEMS_FOR_TASK * (idx + 1)]
            add_lrs_task(lrs_instance, ids_for_task, idx, TYPE_EXPORT_REPORTS)
    if reqs_list:# Количество параллельных задач для требований
        tasks_count_reqs = len(reqs_list) // ITEMS_FOR_TASK
        tasks_count_reqs += 1 if len(reqs_list) % ITEMS_FOR_TASK != 0 else 0
        for idx in range(tasks_count_reqs):
            ids_for_task = reqs_list[ITEMS_FOR_TASK * idx: ITEMS_FOR_TASK * (idx + 1)]
            add_lrs_task(lrs_instance, ids_for_task, tasks_count_reports + idx, TYPE_EXPORT_REQUIREMENTS)

    lrs_instance.CloseParallelBlock()

    merge_task = workflow.Task()
    merge_task.description = "Формирование итогового архива"
    merge_task.service = SERVICE_NAME
    merge_task.SetTimeout(60 * 8)  # Таймаут 8 часов

    # Правим рекорд фильтра для будущего файла параметров
    names_list = get_orgs_shortnames(params.Get('ИдОрганизаций'), kit_list + reqs_list)
    params.AddArrayString('Организации')
    params.Remove('ИдОрганизаций')
    params.Set('Организации', names_list)

    merge_task.SetMethod('Complect.MergeExport', tasks_count_reports + tasks_count_reqs, params)
    merge_task.user = Session.UserID()
    lrs_instance.AddTask(merge_task)
    lrs_instance.Commit(workflow.CommitPolicy.cpIMMEDIATELY)
    lrs_instance.SetErrback('Complect.FullExportErrback', tasks_count_reports + tasks_count_reqs)
    lrs_instance.SetErrbackUser(Session.UserID())
    lrs_instance.Finalize()

    return kit_list


def add_lrs_task(lrs_instance, ids_for_task, idx, task_type):
    """
    Добавляет задачу в dwc
    :param lrs_instance: инстанс сервиса
    :param ids_for_task: идентификаторы комплектов
    :param idx: Порядковый номер операции
    :param task_type: Тип выгрузки (отчеты или требования)
    :return:
    """
    LogMsg('ids_for_task: ' + str(ids_for_task))
    lrs_instance.BeginBranch()
    task = workflow.Task()
    task.SetLimit('export_task:' + str(Session.ClientID()), TASKS_LIMIT)
    task.description = "Выгрузка в архив ч." + str(idx + 1)
    task.service = SERVICE_NAME
    task.SetMethod('Complect.TaskExport', ids_for_task, idx + 1, task_type)
    task.user = Session.UserID()
    lrs_instance.AddTask(task)
    lrs_instance.EndBranch()


def get_orgs_shortnames(orgs_ids, kit_list):
    """
    Получаем имена организаций по идентификаторам организаций или по комплектам
    :param orgs_ids:
    :param kit_list:
    :return:
    """
    if orgs_ids:
        name_list = SqlQuery('''SELECT "ShortName" FROM "Контрагент" WHERE "@Лицо" = ANY($1)''', orgs_ids)
    else:
        name_list = SqlQuery('''SELECT
                                    contragent."ShortName"
                                FROM
                                    "Комплект" doc
                                    JOIN "Контрагент" as contragent on doc."НашаОрганизация"=contragent."@Лицо"
                                WHERE doc."@Документ" = ANY($1)
                                GROUP BY contragent."ShortName"''', kit_list)
    return name_list.ToList('ShortName')


def is_need_show_help_link(params):
    """
    Проверяем надо ли показывать ссылку на help.sbis.ru, где говорится, что зашифрованное не выгружаем
    :param params:
    :return:
    """
    kit_list = Документ.ВыгрузитьОтчетыИПисьма(None, params, None, None)
    if not kit_list:
        empty_format = Record()
        empty_format.AddBool('found')
        rs = CreateRecordSet(empty_format.Format())
        empty_format['found'] = False
        rs.InsRow(rs.Size(), empty_format)
        return rs

    # Получить все внешдоки с типом 3, 5, 9 и по ним документы, по ним сверить с списком на выгрузку
    sql = """
            WITH docs AS
            (
               SELECT COALESCE(doc."Раздел", doc."@Документ") as key
               FROM "ВерсияВнешнегоДокумента" vvd
                  left join "ВнешнийДокумент" vd on vvd."ВнешнийДокумент" = vd."@ВнешнийДокумент"
                  left join "Документ" doc on doc."@Документ" = vd."Документ"
               WHERE vvd."Тип" IN (3,5,9)
            )
            SELECT COUNT(key) > 0 as "found"
            FROM docs
            WHERE key = ANY($1::INT[])
            LIMIT 1
          """
    return SqlQuery(sql, kit_list.ToList('@Документ'))


def __get_requirements(params):
    """
    Метод получает идентификаторы требований
    :param params:
    :return:
    """
    result = []
    req_filter = Record()
    req_filter.AddDate('ДатаНачала', params.Get('ДатаС', None))
    req_filter.AddDate('ДатаКонца', params.Get('ДатаП', None))
    req_filter.AddArrayInt32('Получатель')
    req_filter['Получатель'].From(params.Get('ИдОрганизаций', []))
    req_filter.AddString('ФильтрСостояниеКраткое', 'DECRYPTED_ONLY')
    req_filter.AddString('ФильтрВидСущности', 'REQ')
    req_filter.AddBool('docsOnly', True)
    req_list = Requirement.List(None, req_filter, None, Navigation(1000, 0, True))
    LogMsg('требования:' + str(req_list))
    if req_list:
        result = req_list.ToList('@Документ')
    return result


def __get_complects(params, reqs_list):
    """
    метод получает идентификаторы комплектов
    :param params:
    :param reqs_list:
    :return:
    """
    try:
        kit_list = Документ.ВыгрузитьОтчетыИПисьма(None, params, None, None).ToList('@Документ')
    except Error:
        kit_list = []
    if reqs_list:
        kit_list += get_all_notices_by_requirement(reqs_list).ToList('notice_ids')

    return kit_list


def get_all_notices_by_requirement(req_ids):
    """
    По идентификаторам требований получает уведомления
    :param req_ids:
    :return:
    """
    sql = """
            SELECT notice_link."ДокументСледствие" AS "notice_ids"
            FROM "СвязьДокументов" req_link
            JOIN "СвязьДокументов" notice_link ON req_link."ДокументСледствие" = notice_link."ДокументОснование"
            JOIN "Комплект" comp ON comp."@Документ" = notice_link."ДокументСледствие"
            AND comp."ТипДокумента" = 'ПредставлениеФНС'
            AND comp."ПодТипДокумента" = '1125045'
            WHERE req_link."ДокументОснование" = ANY($1);
          """
    return SqlQuery(sql, req_ids)


def export_errback(tasks_count):
    try:
        # список параметров
        params_list = []
        for idx in range(tasks_count):
            params_list.append('export_archive_part_' + str(idx + 1))

        # С сервиса параметров получаем ссылки на архивы
        ep = EndPoint(service_name='parameters')
        json_params = ep.Parameter.GetMultiple(SCOPE_TYPE, str(Session.ClientID()), params_list)

        file_info = RecordSet()
        for item in json_params:
            file_rec = CreateRecord(item.Get('Value'), 2)
            if not file_info:
                file_info = RecordSet(file_rec.Format())
            file_info.AddRow(file_rec)

        # Подчищаем за собой удаляя файлы
        for item in file_info:
            link = item.Get('file_id')
            FileTransfer.Delete(link, 'export_eo_storage')
        # Подчищаем за собой удаляя параметры
        for param in params_list:
            ep.Parameter.Erase(param, SCOPE_TYPE, str(Session.ClientID()))
    except Error as err:
        WarningMsg('В обработчике ошибок произошла ошибка: ' + err)


def merge_archive_parts(tasks_count, export_params):
    """
    Формирует итоговый или итоговые архивы по 2ГБ
    :param tasks_count: количество запущенных задач
    :param export_params: фильтр выгрузки
    :return:
    """
    # список параметров
    LogMsg('export_params: ' + str(export_params))
    params_list = []
    for idx in range(tasks_count):
        params_list.append('export_archive_part_' + str(idx + 1))

    # С сервиса параметров получаем ссылки на архивы
    ep = EndPoint(service_name='parameters')
    json_params = ep.Parameter.GetMultiple(SCOPE_TYPE, str(Session.ClientID()), params_list)
    LogMsg('GetMultiple: ' + str(json_params))
    file_info = RecordSet()
    final_size = 0
    for item in json_params:
        file_rec = CreateRecord(item.Get('Value'), 2)
        final_size += file_rec.Get('file_size')
        if not file_info:
            file_info = RecordSet(file_rec.Format())
        file_info.AddRow(file_rec)

    LogMsg('Итоговый размер: ' + str(final_size))
    if final_size <= ExportHelpers.EXPORT_PART_SIZE:
        result = __create_final_file(file_info, export_params)
    else:
        result = __create_final_partition_file(file_info, export_params)
    # Подчищаем за собой удаляя параметры
    for param in params_list:
        ep.Parameter.Erase(param, SCOPE_TYPE, str(Session.ClientID()))
    return result


def __create_final_partition_file(file_info, export_params):
    """
    Формирует архив по частям
    :param file_info:
    :param export_params:
    :return:
    """
    parts = __group_files(file_info)
    file_ids = []
    for part_num, zip_part in enumerate(parts):
        export_zip = ExportZip(True)
        for item in zip_part:
            # Получаем сам файл
            link = item.Get('file_id')
            file_part = FileTransfer.Download(link, 'export_eo_storage')
            export_zip.add_zip(file_part)
        # Добавляем в архив json из параметров выгрузки
        params_json = export_params.AsJson(2)
        json_rpc_file = RpcFile()
        json_rpc_file.SetData(str(params_json).encode())
        export_zip.add_file(json_rpc_file, 'export_params.json')
        part_name = __get_zip_name(export_params, part_num + 1)
        file_ids.append(__upload_part(export_zip.get_rpc_file(part_name), part_name))
        export_zip.close_and_flush()
    valid = datetime.datetime.now() + datetime.timedelta(hours=24)
    return Record({
        'ResultTmpl': 'EO/download/ShowExportLinks:showExportLinks',
        'ResultTmplParams': {'result': file_ids},
        'ResultValidUntil': valid
    })


def __group_files(files_rs):
    """
    Группировка архивов на фрагменты итогового
    :param files_rs: рекорд сет с данными архивов
    :return:
    """
    result = []
    # Обратный индекс
    r_idx = files_rs.Size() - 1
    # Сортировка по убыванию размера
    files_rs.SortRows(lambda a, b: a.Get('file_size') > b.Get('file_size'))
    for idx, item in enumerate(files_rs):
        # Рекорд сет для фрагмента
        rs = RecordSet(files_rs.Format())
        result.append(rs)
        rs.AddRow(item)
        # Добавляем во фрагмент файлик пока он не превышает ограничение
        while sum(rs.ToList('file_size')) + files_rs[r_idx].Get('file_size') < ExportHelpers.EXPORT_PART_SIZE:
            rs.AddRow(files_rs[r_idx])
            r_idx -= 1
            # если индексы встретили то фрагменты сформированы
            if idx == r_idx:
                break
        if idx == r_idx:
            break
    return result


def __upload_part(rpc_arch, part_name):
    """
    Заливает файл на FT и возвращает объект со ссылкой
    :param rpc_arch:
    :param part_name:
    :return:
    """
    file_size = str(rpc_arch.Size() // 1024 // 1024) + ' мб'
    link = FileTransfer.Upload(rpc_arch, 'export_eo_storage')
    return {'fileUuid': link, 'fileName': part_name, 'fileSize': file_size}


def __create_final_file(file_info, export_params):
    """
    Обьединяет все полученные в задачах  экспорта архивы в один
    :param file_info:
    :param export_params:
    :return:
    """
    export_zip = ExportZip(False)
    # Скачиваем архивчики и формируем один
    for item in file_info:
        link = item.Get('file_id')
        file_part = FileTransfer.Download(link, 'export_eo_storage')
        export_zip.add_zip(file_part)
    # Добавляем в архив json из параметров выгрузки
    params_json = export_params.AsJson(2)
    json_rpc_file = RpcFile()
    json_rpc_file.SetData(str(params_json).encode())
    export_zip.add_file(json_rpc_file, 'export_params.json')
    file_name = __get_zip_name(export_params)
    rpc_arch = export_zip.get_rpc_file(file_name)
    file_data = FileTransfer.Upload(rpc_arch, 'export_eo_storage')
    link = PrepareGetRPCInvocationURL('FileTransfer.Download',
                                      {'id': file_data, 'storage': 'export_eo_storage'},
                                      0,
                                      '/file-transfer/service/')
    export_zip.close_and_flush()
    valid = datetime.datetime.now() + datetime.timedelta(hours=24)

    record = Record()
    record.AddString("ResultLink", link)
    record.AddString("ResultMessage", "Архив готов")
    record.AddDateTime("ResultValidUntil", valid)
    return record


def __get_zip_name(export_params, part_num=0):
    """
    Генерируем имя архива вида "ВыгрузкаОтчетности 1_0 Имя_1, Имя_2 и еще N"
    :param export_params:
    :param part_num:
    :return:
    """
    file_name = 'ВыгрузкаОтчетности 1_0 ' + ', '.join(export_params.Get('Организации')[0:2])
    file_name = re.sub('\\"', '«', file_name)
    file_name = re.sub(r'([а-яА-Я\d])«', r'\1»', file_name)
    # Если в списке больше 2 названий то прибавляем "еще N"
    if len(export_params.Get('Организации')) > 2:
        file_name += ' и еще ' + str(len(export_params.Get('Организации')) - 2)
    if part_num:
        file_name += ' - ' + str(part_num)
    file_name += '.zip'
    return file_name


class FullExport:
    """
    Массовая выгрузка комплектов
    """
    def __init__(self, ids_list, task_num):
        self.documents_ids = ids_list
        self.task_num = task_num
        self.dispatches = RecordSet
        self.ext_documents = RecordSet
        self.authors = RecordSet
        self.archive = None
        self.stream = None
        self.use_disk = False
        self.path_to_zip = ''
        self.filenames_all = set()

    def export_reports(self):
        """
        Главный метод выгрузки отчетов
        :return:
        """
        complects = self.__get_documents_info(self.documents_ids)
        LogMsg('complects_info: ' + str(complects))

        for complect_id in self.documents_ids:
            try:
                ComplectSwitcher.ПроверитьГотовностьОтправок(complect_id, None)
            except Error:
                WarningMsg('Неудалось сформировать отправки: ' + str(complect_id))

        dispatches = self.__get_sendings_info()
        if dispatches.Size() == 0:
            return
        LogMsg('send_docs: ' + str(dispatches))
        self.__get_authors(complects, dispatches)
        LogMsg('authors: ' + str(self.authors))
        self.ext_documents = self.__get_all_docs(dispatches.ToList('@Документ'))
        LogMsg('all_docs: ' + str(self.ext_documents))

        self.__open_zip()
        self.__write_zip(dispatches, complects)
        self.__upload_to_storage()

    def export_requirements(self):
        """
        Главный метод выгрузки трребований
        ВЫгружает требования, по требования получает представления и выгружает их
        :return:
        """
        requirements = self.__get_documents_info(self.documents_ids)
        LogMsg('requirements_info: ' + str(requirements))
        additional_info = self.__get_sendings_info(False)
        self.__get_authors(requirements, additional_info)
        LogMsg('authors: ' + str(self.authors))
        self.ext_documents = self.__get_all_docs(requirements.ToList('@Документ'))
        LogMsg('all_docs: ' + str(self.ext_documents))
        self.__open_zip()
        self.__write_zip(additional_info, requirements, '@Документ')

        # Получаем Представления под экспорт
        self.documents_ids = self.__get_represents(requirements.ToList('@Документ'))
        if self.documents_ids:
            # Основная инфа представлений
            represents = self.__get_documents_info(self.documents_ids)
            LogMsg('represents: ' + str(represents))
            # Получаем список представлений с доп инфой
            represents_info = self.__get_sendings_info(False)
            LogMsg('represents_info: ' + str(represents_info))
            self.ext_documents = self.__get_all_docs(represents.ToList('@Документ'), True)
            LogMsg('represents: all_docs: ' + str(self.ext_documents))
            self.__write_zip(represents_info, represents, '@Документ')
        self.__upload_to_storage()

    def __open_zip(self):
        """
        Метод инициализации зипа
        :return:
        """
        if not self.archive:
            self.archive = ExportZip(False)

    def __write_zip(self, documents, parent_docs=None, link='ИдКомплекта'):
        """
        Формирует архив и заливает на FT
        :return:
        """
        for send_item in documents:
            try:
                curr_complect = send_item if parent_docs is None else parent_docs.Filter({'@Документ': send_item.Get(link)})[0]
                self.__filter_complect_and_send(curr_complect, send_item)
                send_vvd = self.__get_docs_by_dispatch(send_item, curr_complect)
                if send_vvd:
                    full_path = FullExport.__gen_path(curr_complect, send_vvd[0])
                    LogMsg('send_vvd' + str(send_vvd))
                    files_info = {}
                    for idx, vvd in enumerate(send_vvd):
                        info = {}
                        try:
                            FullExport.__filter_ext_docs(vvd, send_item)
                            self.__write_file('ВерсияВнешнегоДокумента', vvd.Get('@ВерсияВнешнегоДокумента'),
                                              vvd, full_path, info)
                            if vvd.Get('@ЭЦП'):
                                self.__write_file('ЭЦП', vvd.Get('@ЭЦП'), vvd, full_path, info)
                        except Error as err:
                            # Если не получилось записать файл или еще где-то споткнулись по пути то попадаем юсда
                            if str(err) == 'ВерсияВнешнегоДокумента':
                                send_vvd.Set(idx, 'NeedExport', False)
                            elif str(err) == 'ЭЦП':
                                send_vvd.Set(idx, '@ЭЦП', None)
                            WarningMsg('Неудалось получить файл. Тип: ' + str(err))
                        except ValueError:
                            # Если не прошли фильтр по внешдокам попадаем сюда
                            send_vvd.Set(idx, 'NeedExport', False)
                        finally:
                            vvd_key = str(vvd.Get('@ВерсияВнешнегоДокумента')) + '_' + str(vvd.Get('СобытиеПоЭЦП'))
                            files_info[vvd_key] = info
                    json = DescriptionGenerator(curr_complect,
                                                send_item,
                                                documents.Filter({'ИдКомплекта': curr_complect.Get('@Документ')}),
                                                send_vvd,
                                                files_info,
                                                self.authors).generate()
                    path = os.path.join(full_path, 'report description.json')
                    self.archive.add_file(json, path)
            except Error:
                WarningMsg('Shit happens for send_doc with id: ' + str(send_item.Get('@Документ')))
                continue

    def __write_file(self, object_name, object_id, vvd, full_path, info):
        """
        Скачиваем файл и добавляем в архив
        :param object_name: Имя обьекта
        :param object_id: Идентификатор файла
        :param vvd: рекорд с информацией о внешдоке
        :param full_path: путь до директории с отправкой
        :param info: обьект дополнительной информации а файле(для json)
        :return:
        """
        try:
            file = BLObject(object_name).Invoke('СохранитьОдинФайл', object_id)
            src_filename = vvd.Get('ИмяФайла') if object_name == 'ВерсияВнешнегоДокумента' else file.Name()
            filename = ExportHelpers.fix_file_name(src_filename, None)
            filename = ExportHelpers.unique_filename(filename, self.filenames_all, full_path)
            path = os.path.join(full_path, filename)
            self.archive.add_file(file, path)
            self.filenames_all.add(path)
            if object_name == 'ЭЦП':
                info['ecp_size'] = file.Size()
                info['ecp_src_name'] = src_filename
                info['ecp_name'] = filename
            else:
                info['vvd_name'] = filename
                info['vvd_size'] = file.Size()
        except Error:
            raise Error(object_name)

    def __upload_to_storage(self):
        """
        Заливает файл в хранилище
        :return:
        """
        temp_archive_name = 'export_temp_{}.zip'.format(str(uuid.uuid4()))
        rpc_arch = self.archive.get_rpc_file(temp_archive_name)
        file_id = FileTransfer.Upload(rpc_arch, 'export_eo_storage')
        self.archive.close_and_flush()

        result_info = Record()
        result_info.AddString('file_id', file_id)
        result_info.AddInt64('file_size', rpc_arch.Size())
        param_name = 'export_archive_part_' + str(self.task_num)
        ep = EndPoint(service_name='parameters')
        ep.Parameter.Set(param_name, result_info.AsJson(2), 3, str(Session.ClientID()))

    def __get_docs_by_dispatch(self, dispatch, complect):
        """
        Получить внешние документы по отправке
        Для пенсий живет костыль, чтьо выгружаем xml вместо pdf
        :param dispatch: Отправка
        :param complect: Комплект
        :return:
        """
        doc_list = self.ext_documents.Filter({'Документ': dispatch.Get('@Документ')})
        # Пенсия
        if (complect.Get('ТипДокумента', '') == 'ПенсияПФР'
                and complect.Get('ПодТипДокумента', '') == 'ОтправкаНаПенсию'
                and dispatch.Get('СостояниеКраткое') == 0):
            complect_docs = self.__get_all_docs([complect.Get('@Документ')])
            # Ищем документ пенсии
            res = doc_list.Filter({'ПодТипДокумента': 'ОтправкаНаПенсию'})[0]
            for i in range(complect_docs.Size()):
                if complect_docs[i].Get('ПодТипДокумента', '') == 'ОтправкаНаПенсию':
                    complect_docs.Set(i, '@ВнешнийДокумент', res.Get('@ВнешнийДокумент', ''))
                    complect_docs.Set(i, '@ВерсияВнешнегоДокумента', res.Get('@ВерсияВнешнегоДокумента', ''))
                    complect_docs.Set(i, 'ИмяФайла', res.Get('ИмяФайла', ''))
            return complect_docs
        return doc_list

    @staticmethod
    def __filter_ext_docs(document, send_doc):
        """
        Фильтр по документам. Не выгружаем чеки ПФ. Не выгружаем Протоколы УП у незапущенных отправок
        :param document:
        :return:
        """
        if document.Get('ПодТипДокумента') in ['протоколCheckXMLUFA', 'протоколCheckXML']:
            raise ValueError
        if not document.Get('@Событие') and document.Get('ПодТипДокумента') == 'ПротоколУПП':
            raise ValueError
        # какие-либо незавершенные события отбриваем
        if send_doc.Get('СостояниеКраткое') != 0 and not document.Get('Конец'):
            # Комплекты на подписании запущенны в ДО, но у них еще нет событий. Их не отбриваем
            if (send_doc.Get('СостояниеКраткое') != ExportHelpers.WAIT_SIGN_STATUS_CODE
                    and send_doc.Get('СостояниеКраткое') != ExportHelpers.ERROR_STATUS_CODE):
                raise ValueError
        # Для СЗВ-М отправленных на подписание не должно выгружаться описание сведений
        if send_doc.Get('СостояниеКраткое') == ExportHelpers.WAIT_SIGN_STATUS_CODE:
            if document.Get('ТипДокумента') == 'СлужебПФР' and document.Get('ПодТипДокумента') == 'описаниеСведений':
                raise ValueError
        return False

    @staticmethod
    def __filter_complect_and_send(complect, send_doc):
        """
        Фильтр комплектов и отправок
        :param complect:
        :param send_doc:
        :return:
        """
        doc_type = complect.Get('ТипДокумента') or complect.Get('Документ.ТипДокумента')
        if not TYPES_DICT.get(doc_type):
            raise Error('Плохой тип комплекта')
        if send_doc.Get('СостояниеКраткое') == ExportHelpers.ENCODE_STATUS_CODE:
            raise Error('Комплект Импортирован из оператора')

    @staticmethod
    def __gen_path(complect, vvd_info):
        """
        Генерирует путь до отправки
        :param complect: инфа комплекта
        :param vvd_info: инфа внешдока
        :return:
        """
        org_path = complect.Get('ИНН')
        if complect.Get('КПП'):
            org_path += '-' + complect.Get('КПП')

        edo_id = str(uuid.uuid4())
        if vvd_info.Get('Раздел'):
            LogMsg('Раздел: ' + str(vvd_info.Get('Раздел')))
            event_parent = Событие.Прочитать(vvd_info.Get('Раздел'))
            edo_id = str(event_parent.Get('кодСобытия', ''))

        doc_type = complect.Get('ТипДокумента') or complect.Get('Документ.ТипДокумента')
        path = os.path.join(org_path,
                            TYPES_DICT.get(doc_type, ['', '', 0])[0],
                            TYPES_DICT.get(doc_type, ['', '', 0])[1],
                            complect.Get('ДатаВремяСоздания', None).strftime("%Y"),
                            complect.Get('ДатаВремяСоздания', None).strftime("%m-%d"),
                            FullExport.__get_complect_name(complect) + ' ' + edo_id)

        LogMsg('path: ' + str(path))
        return path

    @staticmethod
    def __get_complect_name(doc):
        """
        Получает имя комплекта
        :param doc: Документ
        :return:
        """
        doc_type = doc.Get('ТипДокумента') or doc.Get('Документ.ТипДокумента')
        doc_subtype = doc.Get('ПодТипДокумента') or doc.Get('Документ.ПодТипДокумента')
        # Для требований просто пишем красивое слово "Требование"
        if doc_type == 'ИстребованиеФНС':
            return 'Требование'
        type_doc = Record()
        type_doc.AddString('ТипДокумента', doc_type)
        type_doc.AddString('ПодТипДокумента', doc_subtype)
        short_name = doc_type
        try:
            name = ТипФормализованногоДокумента.Получить(type_doc)
            LogMsg('ТипФормализованногоДокумента:Получить -- ' + str(name))
            short_name = name.Get('НазваниеКраткое') or doc.Get('Документ.Название') or doc_type
        except Error as err:
            LogMsg('ТипФормализованногоДокумента:Error -- ' + str(err))
        return short_name

    def __get_authors(self, complects, dispatches):
        """
        ПОлучаем все частные лица
        :return:
        """
        faces = []
        faces += dispatches.ToList('ЛицоСоздал')
        faces += complects.ToList('ЛицоСоздал')
        faces += complects.ToList('Лицо2')
        authors_id = list(set(faces))
        if authors_id:
            filter_params = Record()
            filter_params.AddArrayInt32('PrivatePersons')
            filter_params['PrivatePersons'].From(authors_id)
            self.authors = Person.Read(filter_params)

    @staticmethod
    def __get_documents_info(docs_ids):
        """
        Запрос получающий информацию о комплектах по набору ИД
        :return:
        """
        sql = """
                SELECT
                    doc."@Документ",
                    com."Редактируется",
                    com."ТипДокумента",
                    com."ПодТипДокумента",
                    com."ОтчетныйПериод",
                    com."НашаОрганизация",
                    com."Дата",
                    com."Время",
                    doc_ext."ДатаВремяСоздания",
                    doc_ext."Название",
                    com."Параметры",
                    doc."ИдентификаторДокумента",
                    doc."ЛицоСоздал",
                    doc."ДокументНашаОрганизация",
                    doc_ext."Комментарий",
                    doc."Лицо2",
                    com."УполномоченнаяБухгалтерия",
                    rep_per."Код",
                    rep_per."Год",
                    contragent."ИНН",
                    contragent."КПП",
                    --Инфа отправителя
                    sender."ИНН" "ОтправительИНН",
                    sender."КПП" "ОтправительКПП",
                    dt."ТипДокумента" as "Документ.ТипДокумента",
                    dt."ПодТипДокумента" as "Документ.ПодТипДокумента",
                    dt."Название" as "Документ.Название"
                FROM
                    "Документ" doc
                    LEFT JOIN "Комплект" com USING("@Документ")
                    LEFT JOIN "ДокументРасширение" doc_ext USING("@Документ")
                    LEFT JOIN "ТипДокумента" dt ON doc."ТипДокумента" = dt."@ТипДокумента"
                    LEFT JOIN "ОтчетныйПериод" rep_per on com."ОтчетныйПериод" = rep_per."@ОтчетныйПериод"
                    LEFT JOIN "Контрагент" "contragent" on doc."ДокументНашаОрганизация" = contragent."@Лицо"
                    LEFT JOIN "Контрагент" "sender" on com."MainPayer" = sender."@Лицо"
                WHERE doc."@Документ" = ANY($1)
              """
        return SqlQuery(sql, docs_ids)

    def __get_sendings_info(self, by_parent=True):
        """
        Запрос получающий информацию об отправках по набору ИД
        :return:
        """
        sql = """
                SELECT
                    doc."@Документ",
                    doc."Раздел" "ИдКомплекта",
                    doc."ИдРегламента",
                    doc."Лицо3",
                    doc."ЛицоСоздал",
                    doc_ext."НомерКорректировки",
                    doc_ext."Параметры",
                    doc_ext."СостояниеКраткое",
                    doc_ext."ДатаВремяСоздания",
                    dt."ТипДокумента",
                    --Инфа филиала, пока джойню, может можно лучше
                    filial."ИНН" "ФилиалИНН",
                    filial."КПП" "ФилиалКПП",
                    filial."КодФилиала" "ФилиалКод"
                FROM
                    "Документ" doc
                    LEFT JOIN "ДокументРасширение" doc_ext USING("@Документ")
                    LEFT JOIN "ТипДокумента" dt ON doc."ТипДокумента" = dt."@ТипДокумента"
                    LEFT JOIN "Контрагент" "filial" on doc."Лицо3" = filial."@Лицо"
                WHERE
              """
        if by_parent:
            sql += 'doc."Раздел" = ANY($1)'
        else:
            sql += 'doc."@Документ" = ANY($1)'
        return SqlQuery(sql, self.documents_ids)

    @staticmethod
    def __get_all_docs(docs_ids, by_events=False):
        """
        Получает данные о внешдоках и дополнительные данные для формирования JSON
        :param docs_ids: идентификаторы документов
        :param by_events: надо ли получать информацию по таблице Событие(иначе по таблице ВнешнийДокумент)
        :return:
        """
        # сделано ради представлений, если искать по внешдокам то нет информации о скане,
        # потому что к событию привязан другой внешдок со сканом
        table = 'event' if by_events else 'ext_doc'
        sql = """
                WITH identifiers AS (
                    SELECT
                         min( event."@Событие" ) as "@Событие",
                         ECP."Событие" as "СобытиеПоЭЦП",
                         ECP."@ЭЦП",
                         COALESCE ( VVD."ВнешнийДокумент", ( SELECT "ВнешнийДокумент" FROM "ВерсияВнешнегоДокумента" WHERE "@ВерсияВнешнегоДокумента" = VVD."Страницы" ) ) as "ВнешнийДокумент",
                         VVD."@ВерсияВнешнегоДокумента",
                         VVD."Имя"
                    FROM
                        "ВнешнийДокумент" ext_doc
                        INNER JOIN "ВерсияВнешнегоДокумента" as VVD ON VVD."ВнешнийДокумент" = ext_doc."@ВнешнийДокумент"
                        LEFT JOIN "ВнешнийДокументСобытия" as vds ON VVD."@ВерсияВнешнегоДокумента" = VdS."ВерсияВнешнегоДокумента"
                        LEFT JOIN "Событие" as event ON VdS."Событие" = event."@Событие"
                        FULL JOIN "ЭЦП" as ECP ON ECP."ВерсияВнешнегоДокумента" = VVD."@ВерсияВнешнегоДокумента"
                    WHERE
                        {0}."Документ" = ANY($1)
                    GROUP BY VVD."@ВерсияВнешнегоДокумента", ECP."@ЭЦП"
                )
                SELECT
                        identifiers."@Событие"
                      , COALESCE(identifiers."СобытиеПоЭЦП", identifiers."@Событие") as "СобытиеПоЭЦП"
                      , identifiers."@ВерсияВнешнегоДокумента"
                      , identifiers."@ЭЦП"
                      , identifiers."Имя" as "ИмяФайла"
                      , ext_doc."@ВнешнийДокумент"
                      , {0}."Документ"
                      , event."Описание"
                      , sob_ecp."Описание" as "НазваниеСобытия"
                      , (SELECT "кодСобытия" FROM "Событие" WHERE "@Событие" = event."Раздел") as "Документооборот"
                      , event."Начало" as "ДатаВремя"
                      , event."Конец"
                      , event."Раздел"
                      , ext_doc."Идентификатор" as "ИдентификаторВнешнийДокумент"
                      , ext_doc."ТипДокумента"
                      , ext_doc."ПодТипДокумента"
                      , ext_doc."ТипИзКонтейнера"
                      , ext_doc."ОтчетныйПериод"
                      -- Флаг надо ли выгружать внешдок. По умолчанию все надо
                      , TRUE as "NeedExport"
                      , (identifiers."@Событие" is not null) as "WithEvents"
                FROM
                      identifiers
                LEFT JOIN "Событие" as event using("@Событие")
                LEFT JOIN "ВнешнийДокумент" as ext_doc ON identifiers."ВнешнийДокумент" = ext_doc."@ВнешнийДокумент"
                LEFT JOIN "Событие" as sob_ecp ON identifiers."СобытиеПоЭЦП" = sob_ecp."@Событие"
                ORDER BY identifiers."@ВерсияВнешнегоДокумента"
              """.format(table)
        return SqlQuery(sql, docs_ids)

    @staticmethod
    def __get_represents(docs_ids):
        """
        Получаем список представлений пригодных для экспорта. Только те что имею события
        :param docs_ids:
        :return:
        """
        sql = """
                SELECT DISTINCT
                    d."@Документ"
                FROM
                    "ВерсияВнешнегоДокумента" as vvd
                    LEFT JOIN "ВнешнийДокументСобытия" as vds ON vvd."@ВерсияВнешнегоДокумента" = vds."ВерсияВнешнегоДокумента"
                    LEFT JOIN "Событие" as sob ON vds."Событие" = sob."@Событие"
                    LEFT JOIN "Документ" as d ON d."@Документ" = sob."Документ"
                WHERE d."Раздел" = ANY($1)
              """
        return SqlQuery(sql, docs_ids).ToList('@Документ')


class DescriptionGenerator:
    """
    Формирует файл описания json
    """
    def __init__(self, complect, send_doc, dispatches, docs, files_info, authors):
        self.complect = complect
        self.send_doc = send_doc
        self.dispatches = dispatches
        self.docs = docs
        self.sended = self.__is_not_sended(send_doc)
        self.files_info = files_info
        self.authors = authors
        self.edo_id = self.__get_edo_id()

    def generate(self):
        """
        генерирует и возвращает готовый json
        :return:
        """
        self.__prepare_species()
        # Формат рекорда "Пакет"
        packet_format = CreateRecordFormat()
        packet_format.AddString('ИдКомплекта')
        packet_format.AddBool('ЭтоУБ')
        packet_format.AddDateTime('ДатаВремяСоздания')
        packet_format.AddBool('Прочитан')
        packet_format.AddRecord('Расширение')
        packet_format.AddRecord('Автор')
        packet_format.AddRecord('НашаОрганизация')
        packet_format.AddRecord('Отправитель')
        packet_format.AddRecordSet('Участник')
        packet_format.AddRecordSet('Событие')
        description = Record(packet_format)
        description['ИдКомплекта'].From(str(self.complect.Get('ИдентификаторДокумента')))
        description['ЭтоУБ'].From(self.complect.Get('УполномоченнаяБухгалтерия'))
        description['ДатаВремяСоздания'].From(self.complect.Get('ДатаВремяСоздания'))
        # Для требований буду спрашивать Документ.Просмотрен()
        description['Прочитан'].From(self.__is_read())
        description['Расширение'].From(self.__get_extension())
        if self.complect.Get('ЛицоСоздал'):
            description['Автор'].From(self.__get_author(self.complect.Get('ЛицоСоздал')))

        # Формат рекорда "НашаОрганизация" и "Отправитель"
        org_format = CreateRecordFormat()
        org_format.AddString('ИНН')
        org_format.AddString('КПП')
        org = Record(org_format)
        org['ИНН'].From(self.complect.Get('ИНН'))
        if self.complect.Get('КПП'):
            org['КПП'].From(self.complect.Get('КПП'))
        description['НашаОрганизация'].From(org)

        sender = Record(org_format)
        sender['ИНН'].From(self.complect.Get('ОтправительИНН'))
        if self.complect.Get('ОтправительКПП'):
            sender['КПП'].From(self.complect.Get('ОтправительКПП'))
        description['Отправитель'].From(sender)
        description['Участник'].From(self.__get_members(self.complect.Get('@Документ')))
        description['Событие'].From(self.__get_events())

        rpc_file = RpcFile()
        rpc_file.SetData(str(description.AsJson(2)).encode())
        rpc_file.SetName('report description.json')
        return rpc_file

    def __is_read(self):
        """
        получаем фалг "ПРочитан", т.к. Входящие при экспорте только требования, для них спрашиваем это на БЛ
        :return:
        """
        is_read = True
        if self.complect.Get('ТипДокумента', '') == 'ИстребованиеФНС':
            is_read = Документ.Просмотрен(self.complect.Get('@Документ'))
        return is_read

    def __prepare_species(self):
        """
        Метод для подготовки инфы
        Скорее всего это будет просто набор костылей из-за различий отчетов и требований/представлений
        :return:
        """
        # 'ТипДОкумента' берется с комплекта, но представления не живут в этой таблице. Поэтому получаем из таблицы Документ
        if self.complect.Get('ТипДокумента') is None:
            self.complect.Set('ТипДокумента', self.complect.Get('Документ.ТипДокумента'))
        if self.complect.Get('ПодТипДокумента') is None:
            self.complect.Set('ПодТипДокумента', self.complect.Get('Документ.ПодТипДокумента'))
        # ИдентификаторДокумента у Требований и представлений должен быть одинаковый, чтобы представления догружались в свое требование
        if self.complect.Get('ТипДокумента') == 'ПредставлениеФНС' and self.complect.Get('ПодТипДокумента') != '1125045':
            doc_uuid = self._get_req_by_representation(self.complect.Get('@Документ'))
            self.complect.Set('ИдентификаторДокумента', doc_uuid)
        # есть особые представления с типом ИстребованиеФНС:1184002, но в базе у них другое КНД,
        # для кооректной загрузки оно должно быть такое же как у требования
        if self.complect.Get('ТипДокумента') == 'ПредставлениеФНС':
            if self.docs.Filter({'ТипДокумента': 'ИстребованиеФНС',
                                 'ПодТипДокумента': '1184002',
                                 'ТипИзКонтейнера': 'представление'}).Size() > 0:
                self.complect.Set('ТипДокумента', 'ИстребованиеФНС')
                self.complect.Set('ПодТипДокумента', '1184002')
        # Поля дата и время круче чем ДатаВремяСоздаия, но их нету у представлений
        if self.complect.Get('ТипДокумента') != 'ПредставлениеФНС':
            creation_date = str(self.complect.Get('Дата')) + ' ' + str(self.complect.Get('Время'))
            self.complect.Set('ПодТипДокумента', creation_date)

    def __get_events(self):
        """
        Формирует рекордСет с событиями
        :return:
        """
        # Формат рекорда "Событие"
        events_format = CreateRecordFormat()
        events_format.AddDateTime('ДатаВремя')
        events_format.AddRecord('Документооборот')
        events_format.AddString('Название')
        events_format.AddRecordSet('Документ')
        events_format.AddRecordSet('Участник')
        events_format.AddRecord('НашаОрганизация')
        events = RecordSet(events_format)

        grouped_docs = {}
        tmp_docs = self.docs.GroupBy('СобытиеПоЭЦП')
        for item in tmp_docs:
            evt_data = self.docs.Filter({'СобытиеПоЭЦП': item, 'NeedExport': True, 'WithEvents': self.sended}).as_list()
            if evt_data:
                grouped_docs[item] = evt_data

        members = self.__get_members(self.send_doc.Get('@Документ'), True)

        for evt_id in grouped_docs:
            # Инфа об ивенте
            event_data = grouped_docs[evt_id][0]
            # ищем незавершенные события чтобы пропустить
            not_finished_events = self.docs.Filter({'СобытиеПоЭЦП': event_data.get('СобытиеПоЭЦП'),
                                                    'Конец': None,
                                                    'WithEvents': True})
            if not_finished_events:
                continue
            evt = Record(events_format)
            event_name = event_data.get('НазваниеСобытия') or event_data.get('Описание')
            evt['ДатаВремя'].From(self.__get_event_time(event_data))
            evt['Документооборот'].From(self.__get_edo_rec())
            evt['Название'].From(event_name)
            evt['Документ'].From(self.__get_documents(grouped_docs[evt_id]))
            evt['Участник'].From(members)
            # Формат рекорда "НашаОрганизация" для События
            org_format = CreateRecordFormat()
            org_format.AddString('ИНН')
            org_format.AddString('КПП')
            org_format.AddString('КодФилиала')
            event_org = Record(org_format)
            event_org['ИНН'].From(self.send_doc.Get('ФилиалИНН', ''))
            event_org['КПП'].From(self.send_doc.Get('ФилиалКПП', ''))
            event_org['КодФилиала'].From(self.send_doc.Get('ФилиалКод', ''))
            evt['НашаОрганизация'].From(event_org)

            events.AddRow(evt)
        LogMsg('Ready events: ' + str(events))
        events.SortRows(DescriptionGenerator._sort_events)
        return events

    def __get_event_time(self, data):
        """
        Получает время события, для всех отчетов кроме требований и представлений нужно НАчало события(ДатаВремя)
        :param data:
        :return:
        """
        # Если не запущен в документооборот то ДАтаВремя нету, надо время создания комплекта
        time = data.get('ДатаВремя') or self.complect.Get('ДатаВремяСоздания')
        # Для требований и представлений время Конца события надо
        if self.complect.Get('ТипДокумента') in ['ИстребованиеФНС', 'ПредставлениеФНС']:
            time = data.get('Конец') or data.get('ДатаВремя')
        return time

    def __get_documents(self, vvd_info):
        """
        формирует recordset документов на событии
        :param vvd_info: список внешдоков
        :return:
        """
        # Формат рекорда "Документ"
        doc_format = CreateRecordFormat()
        doc_format.AddString('Название')
        doc_format.AddString('Идентификатор')
        doc_format.AddRecordSet('Файл')
        doc_format.AddRecordSet('Подпись')
        doc_format.AddRecord('Период')
        doc_format.AddString('ХешУникальныхДанных')  # Не заполняю
        doc_format.AddString('Категория')  # Не заполняю
        doc_format.AddRecord('Расширение')  # Не заполняю
        documents = RecordSet(doc_format)

        for vvd in vvd_info:
            LogMsg('__get_documents: vvd - ' + str(vvd))
            doc = Record(doc_format)
            doc['Название'].From(self.__get_doc_name(vvd))
            edo_id = ''
            if vvd.get('ИдентификаторВнешнийДокумент', ''):
                edo_id = str(vvd.get('ИдентификаторВнешнийДокумент', ''))
                edo_id = edo_id.replace('-', '')
            doc['Идентификатор'].From(edo_id)
            vvd_key = str(vvd.get('@ВерсияВнешнегоДокумента')) + '_' + str(vvd.get('СобытиеПоЭЦП'))
            LogMsg('__get_documents: file_info - ' + str(self.files_info[vvd_key]))
            file = self.files_info[vvd_key]
            doc['Файл'].From(self.__get_document_file(vvd, file))

            # Получаем серт
            if vvd.get('@ЭЦП'):
                LogMsg('__get_documents: ЭЦП - ' + str(vvd.get('@ЭЦП')))
                cert = ЭЦП.ПолучитьПодпись(int(vvd.get('@ЭЦП')))
                cert_link = str(cert[0].Get('ИдентификаторДиск', '')) or str(cert[0].Get('Подпись'))
                doc['Подпись'].From(self.__get_file_recordset(cert_link, file['ecp_size'],
                                                              file['ecp_name'], file['ecp_src_name'], True))

            # Для каждого внешдока РСВ получаем отчетный период
            if self.complect.Get('ПодТипДокумента', '') == 'КвартальнаяОтчетность':
                period_id = vvd.get('ОтчетныйПериод')
                if period_id:
                    period_info = ОтчетныйПериод.Прочитать(period_id)
                    period_rec = Record()
                    period_rec.AddInt32('Код', period_info.Get('Код'))
                    period_rec.AddInt32('Год', period_info.Get('Год'))
                    doc['Период'].From(period_rec)

            documents.AddRow(doc)
        return documents

    def __get_document_file(self, vvd, file):
        """
        получает рекордсет "Файл" для документа
        Для отчета из пришедшего события формирует пустой рекордсет
        :param vvd:
        :param file:
        :return:
        """
        if (vvd.get('НазваниеСобытия') in ['РезультатПриемаДекларацияНО', 'протокол', 'запросКвитанция']
                and vvd.get('ТипИзКонтейнера') in ['декларация', 'пачкаРСВ', 'пачкаИС', 'запрос']):
            file_rec = self.__get_file_recordset('', 0, '', '')
        else:
            file_rec = self.__get_file_recordset('', file['vvd_size'], file['vvd_name'], vvd.get('ИмяФайла'))
        return file_rec

    @staticmethod
    def __get_file_recordset(link, size, filename, source_filename, is_ecp=False):
        """
        формирует рекордсет файла или подписи
        :param link: Ссылка (для документов пустота, для подписи либо идентификатор сбисДиска либо бинарные данные)
        :param size: Размер файла
        :param filename: Имя файла в архиве
        :param source_filename: Исходное имя файла (возможно оно изменено из-за повторения)
        :param is_ecp: Сформировать ли данные для поля Подпись
        :return:
        """
        # Формат рекорда "Файл"
        file_format = CreateRecordFormat()
        file_format.AddString('Ссылка')
        file_format.AddInt32('Размер')
        file_format.AddString('Имя')
        file_format.AddString('ИсходноеИмяФайла')
        file_rec = Record(file_format)
        file_rec['Ссылка'].From(link)
        file_rec['Размер'].From(size)
        file_rec['Имя'].From(filename)
        file_rec['ИсходноеИмяФайла'].From(source_filename)
        if is_ecp:
            # Формат рекорда "Подпись"
            cert_format = CreateRecordFormat()
            cert_format.AddRecord('Файл')
            cert_file_rec = Record(cert_format)
            cert_file_rec['Файл'].From(file_rec)
            cert_files = RecordSet(cert_format)
            cert_files.AddRow(cert_file_rec)
            return cert_files

        files = RecordSet(file_format)
        files.AddRow(file_rec)
        return files

    #TODO Что-то сделать
    def __get_doc_name(self, vvd_info):
        """
        Целый метод чтобы получить название документа, т.к. куча частностей
        :param vvd_info: инфа внешдока
        :return:
        """
        name = ''
        # Обычно название документа берем из базы, из поля ТипИзКонтейнера
        if vvd_info.get('ТипИзКонтейнера') is not None:
            name = vvd_info.get('ТипИзКонтейнера')
        if (self.complect.Get('ТипДокумента', '') in ['ПисьмоРОССТАТ', 'ПисьмоФНС', 'ПисьмоПФР']
                or (self.complect.Get('ТипДокумента', '') == 'ПенсияПФР'
                    and self.complect.Get('ПодТипДокумента', '') == 'ОтправкаНаПенсию')):
            if (vvd_info.get('ТипДокумента', '') == 'ПенсияПФР'
                    and vvd_info.get('ПодТипДокумента', '') == 'ОтправкаНаПенсию'):
                name = 'письмо'
            elif not self.sended:
                if vvd_info.get('ТипДокумента', '') in ['ПисьмоРОССТАТ', 'ПисьмоФНС', 'ПисьмоПФР']:
                    # Костыли для обращения и доверенности
                    if vvd_info.get('ПодТипДокумента', '') == '1166102':
                        name = 'обращение'
                    else:
                        name = 'письмо'
                elif (vvd_info.get('ТипДокумента', '') == 'ОтчетФНС'
                      and vvd_info.get('ПодТипДокумента', '') == '1167005'):
                    name = 'доверенность'
                else:
                    name = 'приложениеПисьма'
        # Для приложений с письмах фигачим  - 'письмоПриложение'
        if vvd_info.get('ТипДокумента', '') == 'СлужебныеЭО' and vvd_info.get('ПодТипДокумента', '') == 'ПриложениеЭО':
            name = 'приложениеПисьма'
        # Для события с подписанием отчета инспектором, ставим название на документ - приложение
        if (vvd_info.get('НазваниеСобытия') in ['РезультатПриемаДекларацияНО', 'протокол', 'запросКвитанция']
                and vvd_info.get('ТипИзКонтейнера') in ['декларация', 'пачкаРСВ', 'пачкаИС', 'запрос']):
            name = 'приложение'
        return name

    def __get_extension(self):
        """
        Формирует рекорд Расширение для пакета
        :return:
        """
        extension = Record()
        extension.AddString('ТипДокумента', self.complect.Get('ТипДокумента'))
        extension.AddString('ПодТипДокумента', self.complect.Get('ПодТипДокумента'))
        extension.AddBool('Актуальность', self.__get_actual())
        if self.send_doc.Get('СостояниеКраткое') == ExportHelpers.WAIT_SIGN_STATUS_CODE:
            # Отправленные на подписание переносятся как неотправленные, и должны быть открытые для редактирования
            extension.AddBool('Комплект.Редактируется', True)
        else:
            extension.AddBool('Комплект.Редактируется', self.complect.Get('Редактируется'))

        extension.AddRecord('ОтчетныйПериод')
        if self.complect.Get('Код') is not None:
            # Формат рекорда "ОтчетныйПериод"
            period = Record()
            period.AddInt32('Код', self.complect.Get('Код'))
            period.AddInt32('Год', self.complect.Get('Год'))
            extension['ОтчетныйПериод'].From(period)
        extension.AddInt32('НомерКорректировки', self.send_doc.Get('НомерКорректировки'))

        # Набор частностей
        # Пенсия
        if (self.complect.Get('ТипДокумента', '') == 'ПенсияПФР'
                and self.complect.Get('ПодТипДокумента', '') == 'ОтправкаНаПенсию'):
            filter_params = Record()
            filter_params.AddArrayInt32('PrivatePersons')
            filter_params['PrivatePersons'].From([self.complect.Get('Лицо2')])
            face = Person.Read(filter_params)[0]
            extension.AddString('Имя', face.Get('Name', ''))
            extension.AddString('Фамилия', face.Get('Surname', ''))
            extension.AddString('Отчество', face.Get('Patronymic', ''))
        # Письма + Пенсия
        if self.complect.Get('ТипДокумента') in ['ПисьмоФНС', 'ПисьмоПФР', 'ПисьмоРОССТАТ', 'ПенсияПФР']:
            extension.AddString('ТемаПисьма', self.complect.Get('Название'))
        # Уведомления
        if (self.complect.Get('ТипДокумента', '') == 'ПредставлениеФНС'
                and self.complect.Get('ПодТипДокумента', '') == '1125045'):
            rec_info = DescriptionGenerator.get_req_by_notice(self.complect.Get('@Документ'))
            if rec_info:
                extension.AddString('КомплектОснование', str(rec_info[0]['uuid']))
                extension.AddString('ОснованиеНомер', str(rec_info[0]['number']))
        elif self.complect.Get('ТипДокумента', '') in ['ПредставлениеФНС', 'ИстребованиеФНС']:
            extension.AddString('ОснованиеНомер', str(self.complect.Get('ИдентификаторДокумента')))
        # 6-НДФЛ
        if (self.complect.Get('ТипДокумента', '') == 'ОтчетФНС'
                and self.complect.Get('ПодТипДокумента', '') == '1151099'):
            if self.send_doc.Get('Параметры'):
                param_dict = ParseHstore(self.send_doc.Get('Параметры', ''))
                if param_dict.get('CodeOKTMO'):
                    extension.AddString('Параметры', CreateHstore({'CodeOKTMO': param_dict.get('CodeOKTMO')}))
        # Больничные и Пособия
        if (self.complect.Get('ТипДокумента', '') == 'РеестрФСС'
                and self.complect.Get('ПодТипДокумента', '') == 'Allowance'):
            if self.complect.Get('Параметры'):
                param_dict = ParseHstore(self.complect.Get('Параметры', ''))
                if param_dict.get('Пособие'):
                    extension.AddString('Отчет.ДопДанные', param_dict.get('Пособие'))
        return extension

    def __get_actual(self):
        """
        Определяем актуальность отправки.
        Отправка актуальная если она:
        - неотправлена
        - единственная
        Разводка на кастомное определение актуальности для РСВ-1 и пособий
        :return:
        """
        is_actual = True
        if self.dispatches.Size() < 2 or not self.sended:
            return is_actual
        # свой способ для кварталки
        if self.complect.Get('ПодТипДокумента', '') == 'КвартальнаяОтчетность':
            return self.__quarter_actual()
        # свой способ для пособий
        if self.complect.Get('ПодТипДокумента', '') == 'Allowance':
            return self.__allowance_actual()
        return self.__default_actual()

    def __default_actual(self):
        """
        СТандартное определение актуальности
        Отправка актуальная если она:
        - создана последней для конкретного плательщика
        :return:
        """
        is_actual = True
        curr_face_id = self.send_doc.Get('Лицо3')
        for dispatch in self.dispatches:
            disp_face_id = dispatch.Get('Лицо3')
            if disp_face_id != curr_face_id:
                continue
            if self.send_doc.Get('ДатаВремяСоздания', None) < dispatch.Get('ДатаВремяСоздания', None):
                is_actual = False
        return is_actual

    def __allowance_actual(self):
        """
        определение актуальности для пособий
        Отправка актуальная если она:
        - создана последней для конкретного регламента
        :return:
        """
        is_actual = True
        curr_regulation = self.send_doc.Get('ИдРегламента')
        for dispatch in self.dispatches:
            disp_regulation = dispatch.Get('ИдРегламента')
            if disp_regulation != curr_regulation:
                continue
            if self.send_doc.Get('ДатаВремяСоздания', None) < dispatch.Get('ДатаВремяСоздания', None):
                is_actual = False
        return is_actual

    def __quarter_actual(self):
        """
        Определение актуальности для РСВ-1
        Сравнивается время между отчетами РСВ-1 за один отчетный период, самый последний будет актуальным
        :return:
        """
        is_actual = True
        #TODO Очень не хочется делать лишний запрос, но наверное пришлось
        all_docs = ДокументОтправка.ListExtDocByEvents(self.dispatches.ToList('@Документ'))
        #внешдоки текущей отправки(подготавливаемой в данный момент)
        item_vd = all_docs.Filter({'ИдОтправки': self.send_doc.Get('@Документ', None)})
        if not item_vd:
            return True
        item_vd = item_vd.Filter({'ПодТипДокумента': 'РСВ-1'})
        if not item_vd:
            return True
        item_period = item_vd[0].Get('ОтчетныйПериод', 0)
        for disp in self.dispatches:
            #внешдоки отправки
            disp_vd = all_docs.Filter({'ИдОтправки': disp.Get('@Документ', None)})
            disp_vd = disp_vd.Filter({'ПодТипДокумента': 'РСВ-1'})
            disp_period = disp_vd[0].Get('ОтчетныйПериод', 0)
            if item_period != disp_period:
                continue
            if item_vd[0].Get('Конец', None) < disp_vd[0].Get('Конец', None):
                is_actual = False
        return is_actual

    def __get_edo_rec(self):
        """
        Формирует рекорд для поля Документооборот
        :return:
        """
        # Формат рекорда "Документооборот"
        edo_format = CreateRecordFormat()
        edo_format.AddString('Комментарий')
        edo_format.AddString('Идентификатор')
        edo_format.AddString('Тип')
        edo_format.AddInt32('Состояние')

        edo = Record(edo_format)
        edo['Комментарий'].From(self.complect.Get('Комментарий'))
        edo['Идентификатор'].From(self.edo_id)
        # Запрашиваем тип документооборота с сервиса реглаентов
        edo_type = None
        if self.send_doc.Get('ИдРегламента'):
            regl = regls.GetManager().Get(self.send_doc.Get('ИдРегламента'))
            edo_type = regl.Name()
        # Свои типы для 4 вариантов комплектов
        if self.complect.Get('ТипДокумента') == 'ИстребованиеФНС':
            edo_type = 'Документ'
        if (self.complect.Get('ТипДокумента') == 'ПредставлениеФНС'
                or self.complect.Get('Документ.ТипДокумента') == 'ПредставлениеФНС'):
            edo_type = 'ПредставлениеЭДО'
        if (self.complect.Get('ТипДокумента', '') == 'ПенсияПФР'
                and self.complect.Get('ПодТипДокумента', '') == 'ОтправкаНаПенсию'):
            edo_type = 'НазначениеПенсии'
        if (self.complect.Get('ТипДокумента', '') == 'ПредставлениеФНС'
                and self.complect.Get('ПодТипДокумента', '') == '1125045'):
            edo_type = 'Представление'
        edo['Тип'].From(edo_type)
        edo['Состояние'].From(self.send_doc.Get('СостояниеКраткое'))
        return edo

    def __get_org_rec(self):
        """
        Формирует рекорд НашаОрганизация
        :return:
        """
        # Формат рекорда "НашаОрганизация"
        org_format = CreateRecordFormat()
        org_format.AddString('ИНН')
        org_format.AddString('КПП')
        org = Record(org_format)
        org['ИНН'].From(self.complect.Get('ИНН'))
        if self.complect.Get('КПП'):
            org['КПП'].From(self.complect.Get('КПП'))
        return org

    def __get_members(self, doc_id, with_author=False):
        """
        Формирует рекорд Участник, для Событий с полем Автор
        :param doc_id: Идентификатор документа
        :param with_author: Нужно ли поле Автор
        :return:
        """
        member_format = CreateRecordFormat()
        member_format.AddRecord('Инспекция')
        member_format.AddRecord('ИнспекцияКонечная')
        member_format.AddRecord('Автор')

        inspect_format = CreateRecordFormat()
        inspect_format.AddString('Код')
        inspect_format.AddString('ДопКод')
        inspect_format.AddString('Название')
        inspect_format.AddInt32('ТипИнспекции')

        member = Record(member_format)
        inspect = Record(inspect_format)
        last_inspect = Record(inspect_format)

        inspections = DescriptionGenerator._get_inspection_data(doc_id)
        if not inspections:
            member['Инспекция'].From(inspect)
            member['ИнспекцияКонечная'].From(last_inspect)
            return member
        LogMsg('Инспекшн: ' + str(inspections))
        direct_type = TYPES_DICT.get(self.complect.Get('ТипДокумента', ['', '', 0]))[2]
        inspections = inspections[0]
        inspect['Код'].From(inspections.Get('Инспекция.Код', ''))
        inspect['ДопКод'].From(inspections.Get('Инспекция.ДопКод', ''))
        inspect['Название'].From(inspections.Get('Инспекция.КраткоеНазвание', ''))
        inspect['ТипИнспекции'].From(inspections.Get('Инспекция.Тип', None) or direct_type)
        last_inspect['Код'].From(inspections.Get('ИнспекцияКонечная.Код', ''))
        last_inspect['ДопКод'].From(inspections.Get('ИнспекцияКонечная.ДопКод', ''))
        last_inspect['Название'].From(inspections.Get('ИнспекцияКонечная.КраткоеНазвани', ''))
        last_inspect['ТипИнспекции'].From(inspections.Get('ИнспекцияКонечная.Тип', None) or direct_type)

        member['Инспекция'].From(inspect)
        member['ИнспекцияКонечная'].From(last_inspect)
        if with_author:
            author = self.__get_author(self.send_doc.Get('ЛицоСоздал'))
            member['Автор'].From(author)
        members = RecordSet(member_format)
        members.AddRow(member)
        return members

    def __get_author(self, author_id):
        """
        Формирует рекорд "Автор"
        :param author_id:
        :return:
        """
        # Формат рекорда "Автор"
        author_format = CreateRecordFormat()
        author_format.AddString('Имя')
        author_format.AddString('Фамилия')
        author_format.AddString('Отчество')
        author_rec = Record(author_format)
        author = self.authors.Filter({'PrivatePersonID': author_id})
        if author:
            author_info = author[0]
            author_rec['Имя'].From(author_info.Get('Name', '') or '')
            author_rec['Фамилия'].From(author_info.Get('Surname', '') or '')
            author_rec['Отчество'].From(author_info.Get('Patronymic', '') or '')
        return author_rec

    def __get_edo_id(self):
        """
        Получает идентификатор документооборота
        :return:
        """
        edo_id = ''
        for item in self.docs:
            if item.Get('Документооборот') is not None:
                edo_id = str(item.Get('Документооборот'))
                break
        return edo_id

    @staticmethod
    def __is_not_sended(send_doc):
        """
        ПРоверяет состояние отправки, ошибочна ли она
        :param send_doc:
        :return:
        """
        if send_doc.Get('СостояниеКраткое') in [ExportHelpers.NOT_SEND_STATUS_CODE,
                                                ExportHelpers.ERROR_STATUS_CODE,
                                                ExportHelpers.WAIT_SIGN_STATUS_CODE]:
            return False
        return True

    @staticmethod
    def _sort_events(left, right):
        """
        Сортуруем по времени событий, но бывает так что первичка и протокол единовременны
        тогда надо чтобы первичка была первей
        """
        if left.Get('ДатаВремя', '') == right.Get('ДатаВремя', ''):
            return str(left.Get('Название', '')) in ['ДекларацияНП', 'сведения', 'запрос', 'отчет']
        return left.Get('ДатаВремя', '') < right.Get('ДатаВремя', '')

    @staticmethod
    def _get_req_by_representation(doc_id):
        """
        идентификатор родительского документа по идентификатору вложенного
        :param doc_id: Первичный ключ документа
        :return:
        """
        sql = """
                SELECT
                    "ИдентификаторДокумента"
                FROM
                    "Документ" d
                WHERE
                    d."@Документ" = (SELECT "Раздел" FROM "Документ" WHERE "@Документ" = $1)
              """
        return SqlQueryScalar(sql, doc_id)

    @staticmethod
    def get_req_by_notice(notice_id: int):
        """
        Для уведомлений, получает инфу о документе основании
        :param notice_id:
        :return:
        """
        sql = """
                SELECT
                    vd."Номер" AS "number", doc."ИдентификаторДокумента" AS "uuid"
                FROM
                    "СвязьДокументов" req_link
                JOIN "СвязьДокументов" notice_link ON req_link."ДокументСледствие" = notice_link."ДокументОснование"
                JOIN "Документ" doc ON doc."@Документ" = req_link."ДокументОснование"
                  AND notice_link."ДокументСледствие" = $1
                JOIN "ВнешнийДокумент" vd on vd."@ВнешнийДокумент" = req_link."Номер"::int;
              """
        return SqlQuery(sql, notice_id)

    @staticmethod
    def _get_inspection_data(doc_id: int):
        """
        Получить информацию об испекциях по ид документа
        :param doc_id:
        :return:
        """
        sql = """
                SELECT
                      gi1."Код" AS "Инспекция.Код"
                    , gi1."ДопКод" AS "Инспекция.ДопКод"
                    , gi1."Тип" AS "Инспекция.Тип"
                    , gi1."КраткоеНазвание" AS "Инспекция.КраткоеНазвание"

                    , gi2."Код" AS "ИнспекцияКонечная.Код"
                    , gi2."ДопКод" AS "ИнспекцияКонечная.ДопКод"
                    , gi2."Тип" AS "ИнспекцияКонечная.Тип"
                    , gi2."КраткоеНазвание" AS "ИнспекцияКонечная.КраткоеНазвание"
                FROM "Документ" d
                LEFT JOIN "ГосударственнаяИнспекция" gi1 ON gi1."@Лицо" = d."Лицо1"
                LEFT JOIN "ГосударственнаяИнспекция" gi2 ON  gi2."@Лицо" = COALESCE(d."Лицо2", d."Лицо1")
                WHERE "@Документ" = $1
                LIMIT 1
              """
        return SqlQuery(sql, doc_id)
