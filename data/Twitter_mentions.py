from lib import lib, libObj, libDb

class Twitter_mentions:
  id = ''
  cliente_id = ''
  api_id = ''
  logproc_api_id = ''
  datahorainsert = ''
  created_time = ''
  engagement_rate = ''
  favorite_count = ''
  followers_count = ''
  is_quote_status = ''
  lang = ''
  retweet_count = ''
  status_id = ''
  text = ''
  timezone = ''
  user_user_name = ''
  user_profile_picture = ''
  user_screen_name = ''
  entities_mediaadditional_media_info_monetizable = ''
  entities_media_display_url = ''
  entities_media_expanded_url = ''
  entities_media_id = ''
  entities_media_media_url = ''
  entities_media_media_url_https = ''
  entities_media_type = ''
  entities_media_url = ''
  entities_media_video_info_duration_millis = ''
  entities_user_mentions_id = ''
  entities_user_mentions_name = ''
  entities_user_mentions_screen_name = ''

  def fromJson(self, cliApi, logproc_api_id: int, json):
    def report_get(key: str):
      if key in json:
        return json[key]

      return ''

    def report_get2(key1, key2: str):
      if key1 in json:
        if key2 in json[key1]:
          return json[key1][key2]

      return ''

    def valSql(key: str):
      return lib.valSql(report_get(key))

    def valSql2(key1, key2: str):
      return lib.valSql(report_get2(key1, key2))

    def strSql(key: str):
      return lib.strSql(report_get(key))

    def media(key1: str, key2: str = ''):
      if 'entities' in json:
        if 'media' in json['entities']:
          if key1 in json['entities']['media'][0]:
            if key2 == '':
              return json['entities']['media'][0][key1]
            else:
              if key2 in json['entities']['media'][0][key1]:
                return json['entities']['media'][0][key1][key2]

      return ''

    def mediaStr(key1: str, key2: str = ''):
      res = lib.strSql(str(media(key1, key2)))
      return res

    self.cliente_id = lib.valSql(cliApi.cli_id)
    self.api_id = lib.valSql(cliApi.api_id)
    self.datahorainsert = 'now()'
    self.logproc_api_id = str(logproc_api_id)

    self.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))

    self.engagement_rate = valSql('engagement_rate')
    self.favorite_count = valSql('favorite_count')
    self.followers_count = valSql('followers_count')
    self.is_quote_status = lib.boolSql(report_get('is_quote_status'))
    self.lang = strSql('lang')
    self.retweet_count = valSql('retweet_count')
    self.status_id = strSql('status_id')
    self.text = strSql('text')
    self.timezone = valSql('timezone')
    self.user_user_name = strSql('user_user_name')
    self.user_profile_picture = strSql('user_profile_picture')
    self.user_screen_name = strSql('user_screen_name')

    self.entities_mediaadditional_media_info_monetizable = lib.boolSql(media('additional_media_info', 'monetizable'))
    self.entities_media_display_url = mediaStr('display_url')
    self.entities_media_expanded_url = mediaStr('expanded_url')
    self.entities_media_id = mediaStr('id')
    self.entities_media_media_url = mediaStr('media_url')
    self.entities_media_media_url_https =mediaStr('media_url_https')
    self.entities_media_type = mediaStr('type')
    self.entities_media_url = mediaStr('url')
    self.entities_media_video_info_duration_millis = mediaStr('video_info', 'duration_millis')

    #Comentado pois para usitlizar entities_user_mentions é necessário criar uma subtabela
    # self.entities_user_mentions_id = lib.strSql(str(json['entities']['user_mentions'][0]['id']))
    # self.entities_user_mentions_name = lib.strSql(str(json['entities']['user_mentions'][0]['name']))
    # self.entities_user_mentions_screen_name = lib.strSql(str(json['entities']['user_mentions'][0]['screen_name']))

  def getInsert(self):
    def fieldV(value):
      return str(value) + ', '

    query = """
          INSERT INTO minter.twitter_mentions
          (cliente_id, api_id, logproc_api_id, datahorainsert, created_time, engagement_rate, favorite_count, followers_count, is_quote_status, lang, retweet_count, status_id, text, timezone, user_user_name, user_profile_picture, user_screen_name, entities_mediaadditional_media_info_monetizable,      entities_media_display_url, entities_media_expanded_url, entities_media_id, entities_media_media_url, entities_media_media_url_https, entities_media_type, entities_media_url, entities_media_video_info_duration_millis) values (""" + \
        fieldV(self.cliente_id) + \
        fieldV(self.api_id) + \
        fieldV(self.logproc_api_id) + \
        fieldV(self.datahorainsert) + \
        fieldV(self.created_time) + \
        fieldV(self.engagement_rate) + \
        fieldV(self.favorite_count) + \
        fieldV(self.followers_count) + \
        fieldV(self.is_quote_status) + \
        fieldV(self.lang) + \
        fieldV(self.retweet_count) + \
        fieldV(self.status_id) + \
        fieldV(self.text) + \
        fieldV(self.timezone) + \
        fieldV(self.user_user_name) + \
        fieldV(self.user_profile_picture) + \
        fieldV(self.user_screen_name) + \
        fieldV(self.entities_mediaadditional_media_info_monetizable) + \
        fieldV(self.entities_media_display_url) + \
        fieldV(self.entities_media_expanded_url) + \
        fieldV(self.entities_media_id) + \
        fieldV(self.entities_media_media_url) + \
        fieldV(self.entities_media_media_url_https) + \
        fieldV(self.entities_media_type) + \
        fieldV(self.entities_media_url) + \
        self.entities_media_video_info_duration_millis + \
    """) on conflict(status_id, cliente_id, api_id) do update set """ + \
    """   created_time = """ + self.created_time + """
        , engagement_rate = """ + self.engagement_rate + """
        , favorite_count = """ + self.favorite_count + """
        , followers_count = """ + self.followers_count + """
        , is_quote_status = """ + self.is_quote_status + """
        , lang = """ + self.lang + """
        , retweet_count = """ + self.retweet_count + """
        , status_id = """ + self.status_id + """
        , text = """ + self.text + """
        , timezone = """ + self.timezone + """
        , user_user_name = """ + self.user_user_name + """
        , user_profile_picture = """ + self.user_profile_picture + """
        , user_screen_name = """ + self.user_screen_name + """
        , entities_mediaadditional_media_info_monetizable = """ + self.entities_mediaadditional_media_info_monetizable + """
        , entities_media_display_url = """ + self.entities_media_display_url + """
        , entities_media_expanded_url = """ + self.entities_media_expanded_url + """
        , entities_media_id = """ + self.entities_media_id + """
        , entities_media_media_url = """ + self.entities_media_media_url + """
        , entities_media_media_url_https = """ + self.entities_media_media_url_https + """
        , entities_media_type = """ + self.entities_media_type + """
        , entities_media_url = """ + self.entities_media_url + """
        , entities_media_video_info_duration_millis = """ + self.entities_media_video_info_duration_millis +  """;"""

    return query
