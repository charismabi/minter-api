#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
from datetime import datetime, date, timedelta
import psycopg2
import sys
import os

minter_api_version = '2022.11.29'

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from minterapi.data.Twitter_mentions import Twitter_mentions
from minterapi.data.Tweets import Tweets
from minterapi.data.Posts_facebook import Posts_facebook
from lib import lib, libObj

token = 'UKvl6Bkb5nYmKvTHP8OHyd3uH0HYPfaG'
printSqlActive = False

def printSql(sql: str):
  if printSqlActive:
    print(str)

def raise_exceptionAndContinue(message: str):
  conexao.exceptionAndContinue = True
  message = message + '[Continuar Procedimento]'

  raise Exception(message)


class CliApis:
  cli_id = 0
  cli_nome = ''
  cli_report_id = 0
  cli_plataforma_nome_api = ''
  api_id = 0
  api_nome = ''
  api_api_code = ''
  datainicialapi = ''
  proc_id = ''
  darsh_name = ''
  darsh_name2 = ''
  period_textapi = '' # 'day', 'week', or 'month'
  logproc_id = ''
  logproc_api_id = ''

  def period(self):
    return self.period_textapi[0:1]

class MinterApi:
  # url = 'https://api.minter.io/v1.0/reports/62227fda6ba2b1a5d23ed8ec/instagram/posts/count?date_from=2020-02-28&to_date=2022-03-28&unit=day&skip=0&order_by=engagemen_rate&access_token=UKvl6Bkb5nYmKvTHP8OHyd3uH0HYPfaG'
  headers = {"Accept": "application/json"}

  def get(self, cliApi, _token, date_from, to_date, count = None, skip = None):
    url = cliApi.api_api_code
    url = url.replace('{token}', _token)
    url = url.replace('{clientes.report_id}', cliApi.cli_report_id)
    url = url.replace('{{date_from}}', str(date_from))
    url = url.replace('{date_from}', str(date_from))
    url = url.replace('{{to_date}}', str(to_date))
    url = url.replace('{to_date}', str(to_date))
    url = url.replace('&unit=day', '&unit=' + cliApi.period_textapi)

    if count != None:
      url = url.replace('{count}', str(count))

    if skip != None:
      url = url.replace('{skip}', str(skip))

    try:
      res = requests.get(url, headers=self.headers).json()
    except:
      raise Exception('Falha ao acessar a API clid_id: "' + str(cliApi.cli_id) + '" cli_nome "' + cliApi.cli_nome + '" api_id: "' + str(cliApi.api_id) + '" url: "' + url + '"')

    print('***********************************************************')
    print('::api(Plataforma:' + cliApi.cli_plataforma_nome_api + ', cli_id:' + str(cliApi.cli_id) + ', api_id:' + str(cliApi.api_id) + ', api_Nome:"' + cliApi.api_nome + '-' + cliApi.darsh_name2 + '"):' + url)
    return res

class Logproc:
  id = 0
  datetimeinsert = ''
  datetime_start = ''
  datetime_end = ''
  loginformations = minter_api_version
  status = ''
  errormessage = ''

  def create(self):
    res = conexao.Query("insert into minter.logproc(datetime_start, status, loginformations) values ('now()', 'em execucao', '" + self.loginformations + "') returning id, datetime_start")
    self.id = res[0][0]
    self.datetime_start = res[0][1]

  def endProcess(self, status, errorMessage = ''):
    self.status = status
    self.errormessage = errorMessage
    self.save()

  def endProcessNormal(self):
    self.status = 'finalizado'
    self.save()
  def endProcessError(self, errorMessage: str):
    print('Logproc.endProcessError: "' + errorMessage + '"')
    self.status = 'erro'
    self.errormessage = errorMessage
    self.save()

  def save(self):
    if not self.id:
      raise Exception("Logproc.save - Id não pode ser Nulo")

    res = conexao.Query("UPDATE minter.logproc SET " +
                  "datetime_end= now(), status='" + self.status + "', errormessage= " + lib.strSql(self.errormessage) +
                  " WHERE id=" + str(self.id) +
                  ' returning datetime_end')
    self.datetime_end = res[0][0]

class Logproc_api:
  id = 0
  logproc_id = 0
  cliente_id = 0
  api_id = 0
  datetime_insert = ''
  datetime_start = ''
  datetime_end = ''
  loginformations = ''
  status = ''
  errormessage = ''

  def clear(self):
    self.id = 0
    self.logproc_id = 0
    self.cliente_id = 0
    self.api_id = 0
    self.datetime_insert = ''
    self.datetime_start = ''
    self.datetime_end = ''
    self.loginformations = ''
    self.status = ''
    self.errormessage = ''
  def create(self, cliApis: CliApis):
    self.clear()
    res = conexao.Query("INSERT INTO minter.logproc_api(logproc_id, cliente_id, api_id, datetime_start, status) VALUES(" +
      str(logproc.id) + ", " +
      str(cliApis.cli_id) + ", " +
      str(cliApis.api_id) + ", " +
      "now(), 'em execucao') returning id, datetime_start")

    self.id = res[0][0]
    self.datetime_start = res[0][1]
    cliApis.logproc_id = logproc.id
    cliApis.logproc_api_id = self.id

  def endProcess(self, status, errorMessage = ''):
    self.status = status
    self.errormessage = errorMessage
    self.save()

  def endProcessNormal(self):
    self.status = 'finalizado'
    self.save()
  def endProcessError(self, errorMessage: str):
    print('Logproc_api.endProcessError: "' + errorMessage + '"')
    self.status = 'erro'
    self.errormessage = errorMessage
    self.save()

  def save(self):
    if not self.id:
      raise Exception("Logproc_api.save - Id não pode ser Nulo")

    res = conexao.Query("UPDATE minter.logproc_api SET " +
                  "datetime_end= now(), status='" + self.status + "', errormessage= '" + self.errormessage + "'"
                  " WHERE id=" + str(self.id) +
                  ' returning datetime_end')
    self.datetime_end = res[0][0]

class Posts:
  caption = ''
  comments = ''
  created_time = ''
  eng_rate_by_reach = ''
  engagement = ''
  engagement_rate = ''
  follower_change_24h = ''
  followers = ''
  image = ''
  impressions = ''
  is_promoted = ''
  likes = ''
  link = ''
  location_latitude = ''
  location_location_id = ''
  location_longitude = ''
  location_name = ''
  media_id = ''
  organic_comments = ''
  organic_eng_rate = ''
  organic_eng_rate_by_reach = ''
  organic_impressions = ''
  organic_likes = ''
  organic_reach = ''
  organic_reach_rate = ''
  organic_saves = ''
  organic_video_views = ''
  promoted_comments = ''
  promoted_eng_rate_by_reach = ''
  promoted_impressions = ''
  promoted_likes = ''
  promoted_reach = ''
  promoted_saves = ''
  promoted_video_views = ''
  reach = ''
  reach_rate = ''
  saved = ''
  timezone = ''
  type = ''
  video_views = ''

class Stories:
  datahorainsert = ''
  story_id = ''
  completion_rate = ''
  created_time = ''
  exit_rate = ''
  exits = ''
  follower_change_24h = ''
  followers = ''
  full_view_rate = ''
  image = ''
  impressions = ''
  reach = ''
  reach_rate = ''
  replies = ''
  taps_back = ''
  taps_forward = ''
  timezone = ''
  type = ''

class Mentions_Posts:
  cliente_id = ''
  api_id = ''
  media_id = ''
  datahorainsert = ''
  caption = ''
  comments = ''
  created_time = ''
  image = ''
  is_mentioned = ''
  is_tagged = ''
  likes = ''
  link = ''
  timezone = ''
  type = ''
  username = ''
  replies_created_time = ''
  replies_likes = ''
  replies_text = ''
  replies_username = ''

class Videos:
  cliente_id = ''
  api_id = ''
  datahorainsert = ''
  comment_count = ''
  created_time = ''
  duration = ''
  engagement_rate = ''
  image = ''
  like_count = ''
  link = ''
  media_id = ''
  share_count = ''
  timezone = ''
  title = ''
  video_description = ''
  view_count = ''

class Conexao:
  connection = psycopg2.connect(user="postgres",
                                password="Charisma2022",
                                host="177.92.115.138",
                                port="5432",
                                database="charisma")
  cursor = connection.cursor()
  blockInsert = ''
  blockInsertCount = 0
  blockInsertMax = 100
  listCliApis = []
  exceptionAndContinue = False
  minterApi = MinterApi()
  print('*** Conexão ao Banco realizada com Sucesso ***')
  
  def listCliApis_Load(self):
    query = '''
select cli_id,
       cli_nome,
       cli_report_id,
       api_id,
       api_nome,
       api_code,
       data_inicial_api,
       api_proc_id,
       api_darsh_name,
       api_darsh_name2,
       cli_plataforma_nome_api,
       period_textapi
  from minter.vw_clientes_apis
    '''
    res = self.Query(query)
    self.listCliApis.clear()

    for item in res:
      cliApis = CliApis()
      cliApis.cli_id = item[0]
      cliApis.cli_nome = item[1]
      cliApis.cli_report_id = item[2]
      cliApis.api_id = item[3]
      cliApis.api_nome = item[4]
      cliApis.api_api_code = item[5]
      cliApis.datainicialapi = item[6]

      dataSeguranca = datetime.today().date() - timedelta(days=30) # Refazer os últimos 30 dias para pegar possíveis mudanças nos dados do minter
      # cliApis.datainicialapi = datetime.strptime('2022-01-01', '%Y-%m-%d').date()

      if (cliApis.datainicialapi > dataSeguranca):
        cliApis.datainicialapi = dataSeguranca

      cliApis.proc_id = item[7]
      cliApis.darsh_name = item[8]
      cliApis.darsh_name2 = item[9]
      cliApis.cli_plataforma_nome_api = item[10]
      cliApis.period_textapi = item[11]

      self.listCliApis.append(cliApis)

  def __init__(self):
    self.connection.autocommit = True

  def execute(self, Sql):
    printSql('execute Sql: ' + Sql)
    self.cursor.execute(Sql)
    printSql('execute Concluído')

  def Query(self, Sql):
    printSql('Query Sql: ' + Sql)
    self.cursor.execute(Sql)
    record = self.cursor.fetchall()
    printSql('Query concluído.')
    return record

  def insertDarshBlock(self, api: CliApis, cliente_id, api_id, darsh_name, dataStr, value, str_ = ''):
    if value == 'null':
      value = '0'

    str_ = lib.strSql(str_, True)

    query = "insert into minter.dados (cliente_id, api_id, darsh_name, data, value, str, period) values (" + str(cliente_id) + ", " + str(api_id) + ", '" + darsh_name + "', '" + \
      dataStr + "', " + str(value) + ", " + str_ + ", '" + api.period() + "')\n" + \
      " on conflict(cliente_id, api_id, darsh_name, data, str, period)" + \
      "\n do update set value = " + str(value) + \
      " where dados.cliente_id = " + str(cliente_id) + \
      " and dados.api_id = " + str(api_id) + \
      " and dados.darsh_name = '" + darsh_name + \
      "' and dados.str = " + str_ + \
      " and dados.data = '" + dataStr + "';" + \
      "\n"
    self.insertDarshBlockAdd(query)

  def insertDados_followers_online(self, cliente_id, api_id, dataStr, hora12, hora24, horasindex, diasemana_ingles, diasemana_portugues,
                                   diasemana_portugues_res, disasemanaindex, value):

    query = "insert into minter.dados_followers_online (cliente_id, api_id, data, hora12, hora24, horasindex, diasemana_ingles, diasemana_portugues, diasemana_portugues_res, disasemanaindex, value) values (" + \
      str(cliente_id) + ", " + str(api_id) + ", '" + dataStr + "', '" + hora12 + "', '" + hora24 + "', " + str(horasindex) + ", '" + \
      diasemana_ingles + "', '" + diasemana_portugues + "' , '" + diasemana_portugues_res + \
      "' , " + str(disasemanaindex) + ", " +   str(value) + ")\n" + \
      " on conflict(cliente_id, api_id, data, hora12, diasemana_ingles)" + \
      "\n do update set value = " + str(value) + \
      " where dados_followers_online.cliente_id = " + str(cliente_id) + \
      " and dados_followers_online.api_id = " + str(api_id) + \
      " and dados_followers_online.diasemana_ingles = '" + diasemana_ingles + \
      "' and dados_followers_online.hora12 = '" + hora12 + \
      "' and dados_followers_online.data = '" + dataStr + "';" + \
      "\n"
    self.insertDarshBlockAdd(query)

  def insertDarshBlockAdd(self, query):
    self.blockInsert = self.blockInsert + query
    self.blockInsertCount += 1

    if self.blockInsertCount >= self.blockInsertMax:
      self.insertDarshBlockExec()

  def insertDarshBlockExec(self):

    if (self.blockInsert != ''):
      self.blockInsert = 'do $$ begin \n' + \
        self.blockInsert + \
        '\nend' + \
        '\n$$;'

      self.execute(self.blockInsert)
      self.blockInsertCount = 0
      self.blockInsert = ''

  def process(self, to_date):

    for cliApi in conexao.listCliApis:
      # if cliApi.cli_id != 112: #todo:comentar estas 2 linhas
      #   continue
      #
      # if cliApi.api_id != 112: #todo:comentar estas 2 linhas
      #   continue
      #
      # if cliApi.api_id == 19: #todo:comentar estas 2 linhas
      #   continue
      #
      # if cliApi.api_nome != 'Posts': #todo:comentar estas 2 linhas
      #   continue
      logproc_api.create(cliApi)
      try:
        if cliApi.proc_id == 'padrao':
          self.process_id_padrao(cliApi, to_date)
        elif cliApi.proc_id == 'data-series':
          self.process_proc_data_series(cliApi, to_date)
        elif cliApi.proc_id == 'data-series-names':
          self.process_proc_data_series_names(cliApi, to_date)
        elif cliApi.proc_id == 'matriz3':
          self.process_proc_data_matriz3(cliApi, to_date)
        elif cliApi.proc_id == 'posts':
          self.process_posts(cliApi, to_date)
        elif cliApi.proc_id == 'stories':
          self.process_stories(cliApi, to_date)
        elif cliApi.proc_id == 'mentions-posts':
          self.process_mentios_posts(cliApi, to_date)
        elif cliApi.proc_id == 'videos':
          self.process_videos(cliApi, to_date)
        elif cliApi.proc_id == 'tweets':
          self.process_tweets(cliApi, to_date)
        elif cliApi.proc_id == 'posts_facebook':
          self.process_posts_facebook(cliApi, to_date)
        elif cliApi.proc_id == 'tweets_mentios':
          self.process_tweets_mentios(cliApi, to_date)
        elif cliApi.proc_id == 'str-value':
          self.process_str_value(cliApi, to_date)
        else:
          raise Exception("Api não implementada: " + cliApi.proc_id + " - " + str(cliApi.api_id) + '-' + cliApi.api_nome)

        self.insertDarshBlockExec() #Executa sqls que estavam em formação (values)

        logproc_api.endProcessNormal()
      except Exception as e:
        logproc_api.endProcessError('Erro na execução da API(' + str(cliApi.api_id) + '-' + cliApi.api_nome + '-' + cliApi.darsh_name2 + '): ' + str(e))
        if not conexao.exceptionAndContinue:
          raise Exception(e)

        conexao.exceptionAndContinue = False

    conexao.Query('select * from minter.func_ajusteapivazia()')

  def validate_response_Notallowed(self, response):
    if ('error' in response):
      if 'message' in response['error']:
        if 'NOT ALLOWED' in response['error']['message'].upper():
          raise_exceptionAndContinue('Usuário sem permissão de acesso para esta API')
          return True

      raise_exceptionAndContinue('Falha na API:"' + response['error'] + '"')
      return True

    return False

  def process_id_padrao(self, cliApi, to_date ):
    response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date)
    if self.validate_response_Notallowed(response):
      return

    reports = response['data']['values']

    for darshName in reports:
      dados = reports[darshName]
      print(darshName)

      for dataStr in dados:
        if (not (type(dataStr) is str)):
          continue

        print(dataStr + ':' + str(dados[dataStr]))
        self.insertDarshBlock(cliApi, cliApi.cli_id, cliApi.api_id, darshName, dataStr, dados[dataStr])

      self.insertDarshBlockExec()

  def process_str_value(self, cliApi, to_date ):
    currentDateProcess = cliApi.datainicialapi

    while (currentDateProcess <= to_date):
      response = self.minterApi.get(cliApi, token, currentDateProcess, currentDateProcess)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']['values']

      for darshName in reports:
        dados = reports[darshName]
        print(darshName)

        for valueStr in dados:
          if (not (type(valueStr) is str)):
            continue

          print(valueStr + ':' + str(dados[valueStr]))
          self.insertDarshBlock(cliApi, cliApi.cli_id, cliApi.api_id, darshName, str(currentDateProcess), dados[valueStr], valueStr)

        self.insertDarshBlockExec()
        currentDateProcess += timedelta(days=1)

  def process_proc_data_series(self, cliApi, to_date):
    currentDateProcess = cliApi.datainicialapi

    while (currentDateProcess <= to_date):
      response = self.minterApi.get(cliApi, token, currentDateProcess, currentDateProcess)
      if self.validate_response_Notallowed(response):
        return

      if 'bar' in response:
        reports = response['bar']
        categories = reports['data']['categories']
        series = reports['data']['series'][0]['data']
      else:
        reports = response['data']
        categories = reports['categories']
        series = reports['series'][0]['data']

      if len(series) != len(categories):
        raise Exception("Quantidade de series é diferente da quantidade de categorias")

      for i in range(len(series)):
        categories_value = categories[i]
        series_value = series[i]
        self.insertDarshBlock(cliApi, cliApi.cli_id, cliApi.api_id, cliApi.darsh_name, str(currentDateProcess), series_value, categories_value)
        print('Categories: ' + categories_value + ' - Series: ' + str(series_value))

      currentDateProcess += timedelta(days=1)

  def process_proc_data_series_names(self, cliApi, to_date):
    currentDateProcess = cliApi.datainicialapi

    while (currentDateProcess <= to_date):
      response = self.minterApi.get(cliApi, token, currentDateProcess, currentDateProcess)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      series = reports['series']

      for serie in series:
        gender = serie['name']
        value = serie['y']
        self.insertDarshBlock(cliApi, cliApi.cli_id, cliApi.api_id, cliApi.darsh_name, str(currentDateProcess), value, gender)
        print('Gender: ' + gender + ' - Value: ' + str(value))

      currentDateProcess += timedelta(days=1)

  def process_proc_data_matriz3(self, cliApi, to_date):
    #corrigir 12 am
    arrhoras12 = ['12 am', '1 am', '2 am', '3 am', '4 am', '5 am', '6 am', '7 am', '8 am', '9 am', '10 am', '11 am', '12 pm', '1 pm', '2 pm', '3 pm', '4 pm', '5 pm', '6 pm', '7 pm', '8 pm', '9 pm', '10 pm', '11 pm']
    arrhoras24 = ['12', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '00', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

    arrdiaSemanaIngles = ['Sun', 'Sat', 'Fri', 'Thu', 'Wed', 'Tue', 'Mon']
    arrdiaSemanaPortugues = ['Domingo', 'Sábado', 'Sexta', 'Quinta', 'Quarta', 'Terça', 'Segunda']
    arrdiaSemanaPortuguesRes = ['dom', 'sab', 'sex', 'qui', 'qua', 'ter', 'seg']

    currentDateProcess = cliApi.datainicialapi

    while (currentDateProcess <= to_date):
      response = self.minterApi.get(cliApi, token, currentDateProcess, currentDateProcess)

      if self.validate_response_Notallowed(response):
        return

      report = response['data']

      for item in report:
        index_Horas = item[0]
        index_diasemana = item[1]
        value = item[2]

        if value != None:
          horas12 = arrhoras12[index_Horas]
          horas24 = arrhoras24[index_Horas]
          diaSemanaIngles = arrdiaSemanaIngles[index_diasemana]
          diaSemanaPortugues = arrdiaSemanaPortugues[index_diasemana]
          diasemanaPortuguesRes = arrdiaSemanaPortuguesRes[index_diasemana]

          self.insertDados_followers_online(cliApi.cli_id, cliApi.api_id, str(currentDateProcess), horas12, horas24, index_Horas, diaSemanaIngles,
                                            diaSemanaPortugues, diasemanaPortuguesRes, index_diasemana, value)

          print('Data: ' + str(currentDateProcess) + ' - Horas: ' + str(horas12) + ' - Dia da semana: ' + str(diaSemanaIngles) + ' - Valor: ' + str(value))

      currentDateProcess += timedelta(days=1)

  def process_posts(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum post encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' Posts encontrados')

      posts = Posts()

      for report in reports:
        def report_get(key: str):
          if key in report:
            return report[key]

          return ''

        def report_get2(key1, key2: str):
          if key1 in report:
            if key2 in report[key1]:
              return report[key1][key2]

          return ''

        posts.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
        posts.media_id = lib.strSql(report_get('media_id'))
        posts.caption = lib.strSql(report_get('caption'))
        posts.comments = lib.valSql(report_get('comments'))
        posts.eng_rate_by_reach = lib.valSql(report_get('eng_rate_by_reach'))
        posts.engagement = lib.valSql(report_get('engagement'))
        posts.engagement_rate = lib.valSql(report_get('engagement_rate'))
        posts.follower_change_24h = lib.valSql(report_get('follower_change_24h'))
        posts.followers = lib.valSql(report_get('followers'))
        posts.image = lib.strSql(report_get('image'))
        posts.impressions = lib.valSql(report_get('impressions'))
        posts.is_promoted = lib.boolSql(report_get('is_promoted'))
        posts.likes = lib.valSql(report_get('likes'))
        posts.link = lib.strSql(report_get('link'))
        posts.location_latitude = lib.valSql(report_get2('location', 'latitude'))
        posts.location_location_id = lib.valSql(report_get2('location', 'location_id'))
        posts.location_longitude = lib.valSql(report_get2('location', 'longitude'))
        posts.location_name = lib.strSql(report_get2('location', 'name'))
        posts.organic_comments = lib.valSql(report_get2('organic', 'comments'))
        posts.organic_eng_rate = lib.valSql(report_get2('organic', 'eng_rate'))
        posts.organic_eng_rate_by_reach = lib.valSql(report_get2('organic', 'eng_rate_by_reach'))
        posts.organic_impressions = lib.valSql(report_get2('organic', 'impressions'))
        posts.organic_likes = lib.valSql(report_get2('organic', 'likes'))
        posts.organic_reach = lib.valSql(report_get2('organic', 'reach'))
        posts.organic_reach_rate = lib.valSql(report_get2('organic', 'reach_rate'))
        posts.organic_saves = lib.valSql(report_get2('organic', 'saves'))
        posts.organic_video_views = lib.valSql(report_get2('organic', 'video_views'))
        posts.promoted_comments = lib.valSql(report_get2('promoted', 'comments'))
        posts.promoted_eng_rate_by_reach = lib.valSql(report_get2('promoted', 'eng_rate_by_reach'))
        posts.promoted_impressions = lib.valSql(report_get2('promoted', 'impressions'))
        posts.promoted_likes = lib.valSql(report_get2('promoted', 'likes'))
        posts.promoted_reach = lib.valSql(report_get2('promoted', 'reach'))
        posts.promoted_saves = lib.valSql(report_get2('promoted', 'saves'))
        posts.promoted_video_views = lib.valSql(report_get2('promoted', 'video_views'))
        posts.reach = lib.valSql(report_get('reach'))
        posts.reach_rate = lib.valSql(report_get('reach_rate'))
        posts.saved = lib.valSql(report_get('saved'))
        posts.timezone = lib.valSql(report_get('timezone'))
        posts.type = lib.strSql(report_get('type'))
        posts.video_views = lib.valSql(report_get('video_views'))

        def fieldV(value):
          return str(value) + ', '

        query = """
  INSERT INTO minter.posts
  (media_id, cliente_id, caption, "comments", created_time, eng_rate_by_reach, engagement, engagement_rate, follower_change_24h, followers, image, impressions, is_promoted, likes, link, location_latitude, location_location_id, location_longitude, location_name, organicorganic_comments, organicorganic_eng_rate, organicorganic_eng_rate_by_reach, organicorganic_impressions, organicorganic_likes, organicorganic_reach, organicorganic_reach_rate, organicorganic_saves, organicorganic_video_views, promoted_comments, promoted_eng_rate_by_reach, promoted_impressions, promoted_likes, promoted_reach, promoted_saves, promoted_video_views, reach, reach_rate, saved, timezone, "type", video_views) values (""" + \
          fieldV(posts.media_id) + \
          fieldV(cliApi.cli_id) + \
          fieldV(posts.caption) + \
          fieldV(posts.comments) + \
          fieldV(posts.created_time) + \
          fieldV(posts.eng_rate_by_reach) + \
          fieldV(posts.engagement) + \
          fieldV(posts.engagement_rate) + \
          fieldV(posts.follower_change_24h) + \
          fieldV(posts.followers) + \
          fieldV(posts.image) + \
          fieldV(posts.impressions) + \
          fieldV(posts.is_promoted) + \
          fieldV(posts.likes) + \
          fieldV(posts.link) + \
          fieldV(posts.location_latitude) + \
          fieldV(posts.location_location_id) + \
          fieldV(posts.location_longitude) + \
          fieldV(posts.location_name) + \
          fieldV(posts.organic_comments) + \
          fieldV(posts.organic_eng_rate) + \
          fieldV(posts.organic_eng_rate_by_reach) + \
          fieldV(posts.organic_impressions) + \
          fieldV(posts.organic_likes) + \
          fieldV(posts.organic_reach) + \
          fieldV(posts.organic_reach_rate) + \
          fieldV(posts.organic_saves) + \
          fieldV(posts.organic_video_views) + \
          fieldV(posts.promoted_comments) + \
          fieldV(posts.promoted_eng_rate_by_reach) + \
          fieldV(posts.promoted_impressions) + \
          fieldV(posts.promoted_likes) + \
          fieldV(posts.promoted_reach) + \
          fieldV(posts.promoted_saves) + \
          fieldV(posts.promoted_video_views) + \
          fieldV(posts.reach) + \
          fieldV(posts.reach_rate) + \
          fieldV(posts.saved) + \
          fieldV(posts.timezone) + \
          fieldV(posts.type) + \
          posts.video_views + """) on conflict(cliente_id, media_id) do update set """ + \
  """
   media_id = """ + posts.media_id + """
  , cliente_id = """ + str(cliApi.cli_id) + """
  , caption = """ + posts.caption + """
  , "comments"  = """ + posts.comments + """
  , created_time = """ + posts.created_time + """
  , eng_rate_by_reach = """ + posts.eng_rate_by_reach + """
  , engagement = """ + posts.engagement + """
  , engagement_rate = """ + posts.engagement_rate + """
  , follower_change_24h = """ + posts.follower_change_24h + """
  , followers = """ + posts.followers + """
  , image = """ + posts.image + """
  , impressions = """ + posts.impressions + """
  , is_promoted = """ + posts.is_promoted + """
  , likes = """ + posts.likes + """
  , link = """ + posts.link + """
  , location_latitude = """ + posts.location_latitude + """
  , location_location_id = """ + posts.location_location_id + """
  , location_longitude = """ + posts.location_longitude + """
  , location_name = """ + posts.location_name + """
  , organicorganic_comments = """ + posts.organic_comments + """
  , organicorganic_eng_rate = """ + posts.organic_eng_rate + """
  , organicorganic_eng_rate_by_reach = """ + posts.organic_eng_rate_by_reach + """
  , organicorganic_impressions = """ + posts.organic_impressions + """
  , organicorganic_likes = """ + posts.organic_likes + """
  , organicorganic_reach = """ + posts.organic_reach + """
  , organicorganic_reach_rate = """ + posts.organic_reach_rate + """
  , organicorganic_saves = """ + posts.organic_saves + """
  , organicorganic_video_views = """ + posts.organic_video_views + """
  , promoted_comments = """ + posts.promoted_comments + """
  , promoted_eng_rate_by_reach = """ + posts.promoted_eng_rate_by_reach + """
  , promoted_impressions = """ + posts.promoted_impressions + """
  , promoted_likes = """ + posts.promoted_likes + """
  , promoted_reach = """ + posts.promoted_reach + """
  , promoted_saves = """ + posts.promoted_saves + """
  , promoted_video_views = """ + posts.promoted_video_views + """
  , reach = """ + posts.reach + """
  , reach_rate = """ + posts.reach_rate + """
  , saved = """ + posts.saved + """
  , timezone = """ + posts.timezone + """
  , "type"  = """ + posts.type + """
  , video_views = """ + posts.video_views + """;"""

        self.insertDarshBlockAdd(query)

  def process_stories(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum Stories encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' Stories encontrados')

      stories = Stories()

      for report in reports:
        def report_get(key: str):
          if key in report:
            return report[key]

          return ''

        stories.story_id = lib.strSql(report_get('story_id'))
        stories.completion_rate = lib.valSql(report_get('completion_rate'))
        stories.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
        stories.exit_rate = lib.valSql(report_get('exit_rate'))
        stories.exits = lib.valSql(report_get('exits'))
        stories.follower_change_24h = lib.valSql(report_get('follower_change_24h'))
        stories.followers = lib.valSql(report_get('followers'))
        stories.full_view_rate = lib.valSql(report_get('full_view_rate'))
        stories.image = lib.strSql(report_get('image'))
        stories.impressions = lib.valSql(report_get('impressions'))
        stories.reach = lib.valSql(report_get('reach'))
        stories.reach_rate = lib.valSql(report_get('reach_rate'))
        stories.replies = lib.valSql(report_get('replies'))
        stories.taps_back = lib.valSql(report_get('taps_back'))
        stories.taps_forward = lib.valSql(report_get('taps_forward'))
        stories.timezone = lib.valSql(report_get('timezone'))
        stories.type = lib.strSql(report_get('type'))

        def fieldV(value):
          return str(value) + ', '

        query = """
  INSERT INTO minter.stories
  (cliente_id, story_id, completion_rate, created_time, exit_rate, exits, follower_change_24h, followers, full_view_rate, image, impressions, reach, reach_rate, replies, taps_back, taps_forward, timezone, "type") values (""" + \
          fieldV(str(cliApi.cli_id)) + \
          fieldV(stories.story_id) + \
          fieldV(stories.completion_rate) + \
          fieldV(stories.created_time) + \
          fieldV(stories.exit_rate) + \
          fieldV(stories.exits) + \
          fieldV(stories.follower_change_24h) + \
          fieldV(stories.followers) + \
          fieldV(stories.full_view_rate) + \
          fieldV(stories.image) + \
          fieldV(stories.impressions) + \
          fieldV(stories.reach) + \
          fieldV(stories.reach_rate) + \
          fieldV(stories.replies) + \
          fieldV(stories.taps_back) + \
          fieldV(stories.taps_forward) + \
          fieldV(stories.timezone) + \
          stories.type + """) on conflict(cliente_id, story_id) do update set """ + \
  """
  cliente_id = """ + str(cliApi.cli_id) + """
  , story_id = """ + stories.story_id + """
  , completion_rate = """ + stories.completion_rate + """
  , created_time = """ + stories.created_time + """
  , exit_rate = """ + stories.exit_rate + """
  , exits = """ + stories.exits + """
  , follower_change_24h = """ + stories.follower_change_24h + """
  , followers = """ + stories.followers + """
  , full_view_rate = """ + stories.full_view_rate + """
  , image = """ + stories.image + """
  , impressions = """ + stories.impressions + """
  , reach = """ + stories.reach + """
  , reach_rate = """ + stories.reach_rate + """
  , replies = """ + stories.replies + """
  , taps_back = """ + stories.taps_back + """
  , taps_forward = """ + stories.taps_forward + """
  , timezone = """ + stories.timezone + """
  , type = """ + stories.type + """;"""

        self.insertDarshBlockAdd(query)

  def process_mentios_posts(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      mentios_posts_name = 'Mentions-Posts(' + cliApi.darsh_name2 + ')'

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum ' + mentios_posts_name + ' encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' ' + mentios_posts_name + ' encontrados')

      mentions_Posts = Mentions_Posts()

      for report in reports:
        def report_get(key: str):
          if key in report:
            return report[key]

          return ''

        def report_get2(key1, key2: str):
          if key1 in report:
            if len(report[key1]) > 0:
              if len(report[key1]) > 1:
                raise Exception('Situação não prevista. Foi encontrado mais de 1 "replies" no Json')

              if key2 in report[key1][0]:
                return report[key1][0][key2]

          return ''

        mentions_Posts.cliente_id = lib.valSql(cliApi.cli_id)
        mentions_Posts.api_id = lib.valSql(cliApi.api_id)
        mentions_Posts.media_id = lib.strSql(report_get('media_id'))
        mentions_Posts.datahorainsert = 'now()'
        mentions_Posts.caption = lib.strSql(report_get('caption'))
        mentions_Posts.comments = lib.valSql(report_get('comments'))
        mentions_Posts.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
        mentions_Posts.image = lib.strSql(report_get('image'))
        mentions_Posts.is_mentioned = lib.boolSql(report_get('is_mentioned'))
        mentions_Posts.is_tagged =  lib.boolSql(report_get('is_tagged'))
        mentions_Posts.likes = lib.valSql(report_get('likes'))
        mentions_Posts.link = lib.strSql(report_get('link'))
        mentions_Posts.timezone = lib.valSql(report_get('timezone'))
        mentions_Posts.type = lib.strSql(report_get('type'))
        mentions_Posts.username = lib.strSql(report_get('username'))

        mentions_Posts.replies_created_time = lib.dataTimeToTimeStampSql(report_get2('replies', 'created_time'))
        mentions_Posts.replies_likes = lib.valSql(report_get2('replies', 'likes'))
        mentions_Posts.replies_text = lib.strSql(report_get2('replies', 'text'))
        mentions_Posts.replies_username = lib.strSql(report_get2('replies', 'username'))

        def fieldV(value):
          return str(value) + ', '

        query = """
  INSERT INTO minter.mentions_posts
  (cliente_id, api_id, media_id, datahorainsert, caption, comments, created_time, image, is_mentioned, is_tagged, likes, link, timezone, type, username, replies_created_time, replies_likes, replies_text, replies_username) values (""" + \
          fieldV(mentions_Posts.cliente_id) + \
          fieldV(mentions_Posts.api_id) + \
          fieldV(mentions_Posts.media_id) + \
          fieldV(mentions_Posts.datahorainsert) + \
          fieldV(mentions_Posts.caption) + \
          fieldV(mentions_Posts.comments) + \
          fieldV(mentions_Posts.created_time) + \
          fieldV(mentions_Posts.image) + \
          fieldV(mentions_Posts.is_mentioned) + \
          fieldV(mentions_Posts.is_tagged) + \
          fieldV(mentions_Posts.likes) + \
          fieldV(mentions_Posts.link) + \
          fieldV(mentions_Posts.timezone) + \
          fieldV(mentions_Posts.type) + \
          fieldV(mentions_Posts.username) + \
          fieldV(mentions_Posts.replies_created_time) + \
          fieldV(mentions_Posts.replies_likes) + \
          fieldV(mentions_Posts.replies_text) + \
          mentions_Posts.replies_username + """) on conflict(media_id, cliente_id, api_id) do update set """ + \
  """
    caption = """ + mentions_Posts.caption + """
  , comments = """ + mentions_Posts.comments + """
  , created_time = """ + mentions_Posts.created_time + """
  , image = """ + mentions_Posts.image + """
  , is_mentioned = """ + mentions_Posts.is_mentioned + """
  , is_tagged = """ + mentions_Posts.is_tagged + """
  , likes = """ + mentions_Posts.likes + """
  , link = """ + mentions_Posts.link + """
  , timezone = """ + mentions_Posts.timezone + """
  , type = """ + mentions_Posts.type + """
  , username = """ + mentions_Posts.username + """
  , replies_created_time = """ + mentions_Posts.replies_created_time + """
  , replies_likes = """ + mentions_Posts.replies_likes + """
  , replies_text = """ + mentions_Posts.replies_text + """
  , replies_username = """ + mentions_Posts.replies_username + """;"""

        self.insertDarshBlockAdd(query)

  def process_videos(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      videos_name = 'Videos(' + cliApi.darsh_name2 + ')'

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum ' + videos_name + ' encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' ' + videos_name + ' encontrados')

      videos = Videos()

      for report in reports:
        def report_get(key: str):
          if key in report:
            return report[key]

          return ''

        videos.cliente_id = lib.valSql(cliApi.cli_id)
        videos.api_id = lib.valSql(cliApi.api_id)
        videos.datahorainsert = 'now()'
        videos.comment_count = lib.valSql(report_get('comment_count'))
        videos.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
        videos.duration = lib.valSql(report_get('duration'))
        videos.engagement_rate = lib.valSql(report_get('engagement_rate'))
        videos.image = lib.strSql(report_get('image'))
        videos.like_count = lib.valSql(report_get('like_count'))
        videos.link = lib.strSql(report_get('link'))
        videos.media_id = lib.valSql(report_get('media_id'))
        videos.share_count = lib.valSql(report_get('share_count'))
        videos.timezone = lib.valSql(report_get('timezone'))
        videos.title = lib.strSql(report_get('title'))
        videos.video_description = lib.strSql(report_get('video_description'))
        videos.view_count = lib.valSql(report_get('view_count'))

        def fieldV(value):
          return str(value) + ', '

        if (videos.media_id == '7122972510756375814'):
          print('x')

        query = """
        INSERT INTO minter.videos
        (cliente_id, api_id, datahorainsert, comment_count, created_time, duration, engagement_rate, image, like_count, link, media_id, share_count, timezone, title, video_description, view_count) values (""" + \
                fieldV(videos.cliente_id) + \
                fieldV(videos.api_id) + \
                fieldV(videos.datahorainsert) + \
                fieldV(videos.comment_count) + \
                fieldV(videos.created_time) + \
                fieldV(videos.duration) + \
                fieldV(videos.engagement_rate) + \
                fieldV(videos.image) + \
                fieldV(videos.like_count) + \
                fieldV(videos.link) + \
                fieldV(videos.media_id) + \
                fieldV(videos.share_count) + \
                fieldV(videos.timezone) + \
                fieldV(videos.title) + \
                fieldV(videos.video_description) + \
                videos.view_count + """) on conflict(media_id, cliente_id, api_id) do update set """ + \
  """
    comment_count = """ + videos.comment_count + """
  , created_time = """ + videos.created_time + """
  , duration = """ + videos.duration + """
  , engagement_rate = """ + videos.engagement_rate + """
  , image = """ + videos.image + """
  , like_count = """ + videos.like_count + """
  , link = """ + videos.link + """
  , media_id = """ + videos.media_id + """
  , share_count = """ + videos.share_count + """
  , timezone = """ + videos.timezone + """
  , title = """ + videos.title + """
  , video_description = """ + videos.video_description + """
  , view_count = """ + videos.view_count + """;"""

        self.insertDarshBlockAdd(query)

  def process_tweets(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      videos_name = 'Tweets(' + cliApi.darsh_name2 + ')'

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum ' + videos_name + ' encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' ' + videos_name + ' encontrados')

      tweets = Tweets()

      for report in reports:
        tweets.fromJson(cliApi, logproc_api.id, report)
        query = tweets.getInsert()
        self.insertDarshBlockAdd(query)

  def process_posts_facebook(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      posts_name = 'Posts_Facebook(' + cliApi.darsh_name2 + ')'

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum ' + posts_name + ' encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' ' + posts_name + ' encontrados')

      posts_facebook = Posts_facebook()

      for report in reports:
        posts_facebook.fromJson(cliApi, report)
        query = posts_facebook.getInsert()
        self.insertDarshBlockAdd(query)

  def process_tweets_mentios(self, cliApi:CliApis, to_date ):
    skip = -1
    count = 500

    while True:
      skip += 1
      response = self.minterApi.get(cliApi, token, cliApi.datainicialapi, to_date, count, skip)

      if self.validate_response_Notallowed(response):
        return

      reports = response['data']
      posts_name = 'Tweets_mentios(' + cliApi.darsh_name2 + ')'

      if len(reports) == 0:
        print(str(len(reports)) + ' Nenhum ' + posts_name + ' encontrado, finalizando a API para este cliente')
        break

      print(str(len(reports)) + ' ' + posts_name + ' encontrados')

      twitter_mentions = Twitter_mentions()

      for report in reports:
        twitter_mentions.fromJson(cliApi, logproc_api.id, report)
        query = twitter_mentions.getInsert()
        self.insertDarshBlockAdd(query)

########################################################################################################################

conexao = Conexao()
logproc = Logproc()
logproc_api = Logproc_api()

try:
  logproc.create()
  minterApi = MinterApi()
  conexao.listCliApis_Load()
  # to_date = datetime.strftime(datetime.now(), '%Y-%m-%d')
  to_date = datetime.today().date()
  conexao.process(to_date)

  logproc.endProcessNormal()
except Exception as e:
  logproc.endProcessError('Erro na execução de minter-api.py: ' + lib.strSql(str(e)))
  raise Exception(e)

print('')
print('*** Processo finalizado normalmente ***')
print('')
