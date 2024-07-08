import requests
import psycopg2
import petl as etl
import sqlalchemy as db
import math

_eddyUrl = '<EDDY_URL>'
_user = '<DB_USER>'
_pass = '<DB_PASS>'
_host = '<DB_HOST>'
_port = <DB_PORT>

authHeaders = {
    'Authorization': 'Basic <EDDY_TOKEN>'
}

targetDB = db.create_engine(f"postgresql://{_user}:{_pass}@{_host}:{_port}/etl")

def writeEtlToDB(data, table):
    '''Записать данные etl в БД'''
    try:
        data.todb(targetDB, table, create=True, drop=True, sample=0)      
    except:
        data.todb(targetDB, table, create=True, sample=0)

def apiGetPage(pageNumber, method, params=''):
    '''Получение страницы данных из апи'''
    if pageNumber != 0:
        response = requests.get(_eddyUrl + method + params + str(pageNumber), headers=authHeaders)
    else:
        response = requests.get(_eddyUrl + method, headers=authHeaders)
    if response.status_code == 200:
        return response.json()
    else:
        return f'{response.status_code} : {str(response.json())}'

def getPreviousTicket():
    '''Получение ид последнего тикета, сохраненного в БД'''
    with targetDB.connect() as conn:
        result = conn.execute(db.table('eddy_temp', db.column('last_ticket')).select())
        if result.rowcount != 0:
            return int(result.fetchone()['last_ticket'])
        else:
            return 0

def formDictObj(data, fields):
    '''Формирование объекта тикета'''
    result = []
    if type(data) is dict:
        for item in data['data'].keys():
            obj = {}
            for field in fields:
                # Отделение даты и часа
                if field == 'date_created':
                    obj[field] = data['data'][item][field][0:10]
                    obj[field + '_hour'] = data['data'][item][field][11:13]
                elif field == 'date_updated':
                    obj[field] = data['data'][item][field]
                    obj[field + '_day'] = data['data'][item][field][0:10]
                    obj[field + '_hour'] = data['data'][item][field][11:13]
                else:
                    obj[field] = data['data'][item][field]
            result.append(obj)
        return result
    else:
        print('Wrong data type!')

def getMessages(id_list, update=False):
    '''Получение постов из апи'''
    eddyMessageData = []
    for id in id_list:
        pageData = apiGetPage(0, f'/tickets/{id}/posts')
        for page in range(1, int(pageData['pagination']['total_pages'])+1):
            data = apiGetPage(page, f'/tickets/{id}/posts', '/?page=')
            if type(data) is dict:
                for item in data['data']:
                    obj = {
                        'ticket_id': item['ticket_id'],
                        'id': item['id'],
                        'user_id': item['user_id'],
                        'date_created': item['date_created'][15:] + '-' + item['date_created'][12:14] + '-' + item['date_created'][9:11],
                        'date_created_hour': item['date_created'][:2]
                    }
                    eddyMessageData.append(obj)
            else:
                print('Wrong data type!')
    messageData = etl.fromdicts(eddyMessageData)
    if update:
        messageData.appenddb(targetDB, 'eddy_posts')
    else:
        writeEtlToDB(messageData, 'eddy_posts')

def getTickets(pages, update=False):
    '''Получение тикетов из апи.'''
    eddyTicketsData = []
    for page in range(1, pages+1):
        data = apiGetPage(page, '/tickets', '/?page=')
        fields = ['id', 'date_created', 'date_updated', 'source', 'status_id', 'priority_id', 'type_id', 'owner_id', 'user_id', 'ticket_lock', 'sla_date', 'sla_flag', 'freeze_date', 'freeze', 'rate', 'rate_date', 'deleted']
        eddyTicketsData.extend(formDictObj(data, fields))
    if update:
        ticketData = etl.fromdicts(eddyTicketsData).select(lambda r: r.id > pTicket).sort('id')
        ticketData.appenddb(targetDB, 'eddy_tickets')
    else:
        ticketData = etl.fromdicts(eddyTicketsData).sort('id')
        writeEtlToDB(ticketData, 'eddy_tickets')
    ticketList = list(ticketData.values('id'))
    print('Getting posts of new tickets.')
    getMessages(ticketList)
    print('Posts of new tickets has been exported to DB.')

def checkTickets(metadata, previousTicket):
    '''Проверка последнего тикета в апи и вычисление количества страниц для обработки'''
    lastTicket = list(metadata['data'])[0]
    if previousTicket != 0:
        page = math.ceil((int(lastTicket) - previousTicket) / 30)
        # Сохранение ид последнего тикета
        # Здесь следут использовать Редис или что-то подобное, но я импровизирую с тем что есть
        if page != 0:
            with targetDB.connect() as conn:
                conn.execute(db.table('eddy_temp').delete())
                conn.execute(db.table('eddy_temp', db.column('last_ticket')).insert().values({"last_ticket":lastTicket}))
            return page
        else:
            return 0
    else:
        with targetDB.connect() as conn:
            conn.execute(db.table('eddy_temp', db.column('last_ticket')).insert().values({"last_ticket":lastTicket}))
        return metadata['pagination']['total_pages']

def fillDatabaseUsers(pages):
    '''Заполнение юзеров в БД'''
    eddyUserData = []
    for page in range(1, pages+1):
        data = apiGetPage(page, '/users', '/?page=')
        if type(data) is dict:
            for item in data['data']:
                obj = {
                    'id': item['id'],
                    'name': item['name'].strip().replace('"', '') + ' ' + item['lastname'].strip().replace('"', ''),
                    'email': item['email'],
                    'group_type': item['group']['type']
                }
                eddyUserData.append(obj)
        else:
            print('Wrong data type!')
    userData = etl.fromdicts(eddyUserData).sort('id')
    writeEtlToDB(userData, 'eddy_users')
        
def fillDatabaseStatus():
    '''Первоначальное заполнение БД: статусы'''
    eddyStatusData = []
    dataStatuses = apiGetPage(0, '/statuses')
    if type(dataStatuses) is dict:
        for item in dataStatuses['data']:
            obj = {
                'id': item['id'],
                'name': item['name']['ru']
            }
            eddyStatusData.append(obj)
    statusData = etl.fromdicts(eddyStatusData)
    writeEtlToDB(statusData, 'eddy_statuses')

def checkMessages():
    with targetDB.connect() as conn:
        result = conn.execute(db.table('eddy_tickets', db.column('id'), db.column('date_updated')).select())
        dataDB = result.fetchall()
    apiTickets = []
    for page in range(1, ticketdata['pagination']['total_pages'] + 1):
        dataUpd = apiGetPage(page, '/tickets', '/?page=')
        for item in dataUpd['data'].keys():
            obj = (int(dataUpd['data'][item]['id']), str(dataUpd['data'][item]['date_updated']))
            apiTickets.append(obj)
    diff = set(apiTickets) - set(dataDB)
    updateIds = []
    updateDates = []
    for x in diff:
        updateIds.append(x[0])
        updateDates.append(x[1])
    if len(updateIds) > 0:
        with targetDB.connect() as conn:
            conn.execute(db.table('eddy_posts').delete().where(db.column('ticket_id').in_(updateIds)))
        getMessages(updateIds, True)
        for i in range(0, len(updateIds)):
            with targetDB.connect() as conn:
                conn.execute(db.update(db.table('eddy_tickets', db.column('date_updated'))).values({'date_updated':updateDates[i]}).where(db.column('id') == updateIds[i]))
                conn.execute(db.update(db.table('eddy_tickets', db.column('date_updated_day'))).values({'date_updated_day':updateDates[i][0:10]}).where(db.column('id') == updateIds[i]))
                conn.execute(db.update(db.table('eddy_tickets', db.column('date_updated_hour'))).values({'date_updated_hour':updateDates[i][11:13]}).where(db.column('id') == updateIds[i]))
        print('New posts has been exported to DB.')
    else:
        print('No new posts.')

ticketdata = apiGetPage(0, '/tickets') # Промежуточный объект для выбора сведений пагинации тикетов
userdata = apiGetPage(0, '/users') # Промежуточный объект для выбора сведений пагинации юзеров
pTicket = getPreviousTicket() # Сохранение ид последнего тикета в БД до его перезаписи
newPages = checkTickets(ticketdata, pTicket) # Получаем количество страниц тикетов для обработки
print('Last ticket in DB: ' + str(pTicket) + ' and there are ' + str(newPages) + ' new page(s) of tickets in the API.')

if newPages != 0:
    if newPages == ticketdata['pagination']['total_pages']:
        print('Writing statuses to DB.')
        fillDatabaseStatus()
        print('Statuses has been exported to DB.')
        print('Writing users to DB.')
        fillDatabaseUsers(int(userdata['pagination']['total_pages']))
        print('Users has been exported to DB.')
        print('Writing tickets to DB.')
        getTickets(newPages)
        print('Tickets has been exported to DB.')
    else:
        print('Updating tickets.')
        getTickets(newPages, True)
        print('Tickets has been updated in DB.')
        print('Updating users.')
        fillDatabaseUsers(int(userdata['pagination']['total_pages']))
        print('Users has been updated in DB.')
        print('Checking new posts in previous tickets.')
        checkMessages()
else:
    print('Checking new posts in previous tickets.')
    checkMessages()

print('Work finished!')