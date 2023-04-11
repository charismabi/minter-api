from lib import lib, libObj, libDb

class Posts_facebook:
  id = ''
  post_id = ''
  cliente_id = ''
  api_id = ''
  logproc_id = ''
  logproc_api_id = ''
  datahorainsert = ''
  author_id = ''
  author_name = ''
  author_picture_url = ''
  author_username = ''
  comment_count = ''
  created_time = ''
  eng_rate_by_reach = ''
  engagement_rate = ''
  fan_change_24h = ''
  fans = ''
  impressions = ''
  is_promoted = ''
  like_count = ''
  message = ''
  organic_eng_rate_by_reach = ''
  organic_impressions = ''
  organic_reach = ''
  organic_reach_rate = ''
  organic_video_views = ''
  permalink_url = ''
  post_type = ''
  promoted_eng_rate_by_reach = ''
  promoted_impressions = ''
  promoted_reach = ''
  promoted_reach_rate = ''
  promoted_video_views = ''
  reach = ''
  reach_rate = ''
  share_count = ''
  status_type = ''
  timezone = ''
  video_views = ''
  period = ''
  media_url = ''

  def fromJson(self, cliApi, json):
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

    self.cliente_id = lib.valSql(cliApi.cli_id)
    self.api_id = lib.valSql(cliApi.api_id)
    self.datahorainsert = 'now()'
    self.logproc_api_id = str(cliApi.logproc_api_id)
    self.logproc_id = str(cliApi.logproc_id)
    self.period = lib.strSql(cliApi.period())

    self.post_id = strSql('post_id')
    self.author_id = strSql('author_id')
    self.author_name = strSql('author_name')
    self.author_picture_url = strSql('author_picture_url')
    self.author_username = strSql('author_username')
    self.comment_count = valSql('comment_count')
    self.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
    self.eng_rate_by_reach = valSql('eng_rate_by_reach')
    self.engagement_rate = valSql('engagement_rate')
    self.fan_change_24h = valSql('fan_change_24h')
    self.fans = valSql('fans')
    self.impressions = valSql('impressions')
    self.is_promoted = lib.boolSql(report_get('is_promoted'))
    self.like_count = valSql('like_count')
    self.message = strSql('message')

    self.organic_eng_rate_by_reach = valSql2('organic', 'eng_rate_by_reach')
    self.organic_impressions = valSql2('organic', 'impressions')
    self.organic_reach = valSql2('organic', 'reach')
    self.organic_reach_rate = valSql2('organic', 'reach_rate')
    self.organic_video_views = valSql2('organic', 'video_views')

    self.permalink_url = strSql('permalink_url')
    self.post_type = strSql('post_type')

    self.promoted_eng_rate_by_reach = valSql2('promoted', 'eng_rate_by_reach')
    self.promoted_impressions = valSql2('promoted', 'impressions')
    self.promoted_reach = valSql2('promoted', 'reach')
    self.promoted_reach_rate = valSql2('promoted', 'reach_rate')
    self.promoted_video_views = valSql2('promoted', 'video_views')

    self.reach = valSql('reach')
    self.reach_rate = valSql('reach_rate')
    self.share_count = valSql('share_count')
    self.status_type = strSql('status_type')
    self.timezone = valSql('timezone')
    self.video_views = valSql('video_views')
    self.media_url = 'null'

    if 'attachments' in json:
      if len(json['attachments']) > 0:
        self.media_url = lib.strSql(json['attachments'][0]['image_url'])


  def getInsert(self):
    def fieldV(value):
      return str(value) + ', '


    query = """
          INSERT INTO minter.posts_facebook
          (post_id, cliente_id, api_id, logproc_id, logproc_api_id, datahorainsert, author_id, author_name, author_picture_url, author_username, comment_count, created_time, eng_rate_by_reach, engagement_rate, fan_change_24h, fans, impressions, is_promoted, like_count, message, organic_eng_rate_by_reach, organic_impressions, organic_reach, organic_reach_rate, organic_video_views, permalink_url, post_type, promoted_eng_rate_by_reach, promoted_impressions, promoted_reach, promoted_reach_rate, promoted_video_views, reach, reach_rate, share_count, status_type, timezone, video_views, media_url, "period") values (""" + \
        fieldV(self.post_id) + \
        fieldV(self.cliente_id) + \
        fieldV(self.api_id) + \
        fieldV(self.logproc_id) + \
        fieldV(self.logproc_api_id) + \
        fieldV(self.datahorainsert) + \
        fieldV(self.author_id) + \
        fieldV(self.author_name) + \
        fieldV(self.author_picture_url) + \
        fieldV(self.author_username) + \
        fieldV(self.comment_count) + \
        fieldV(self.created_time) + \
        fieldV(self.eng_rate_by_reach) + \
        fieldV(self.engagement_rate) + \
        fieldV(self.fan_change_24h) + \
        fieldV(self.fans) + \
        fieldV(self.impressions) + \
        fieldV(self.is_promoted) + \
        fieldV(self.like_count) + \
        fieldV(self.message) + \
        fieldV(self.organic_eng_rate_by_reach) + \
        fieldV(self.organic_impressions) + \
        fieldV(self.organic_reach) + \
        fieldV(self.organic_reach_rate) + \
        fieldV(self.organic_video_views) + \
        fieldV(self.permalink_url) + \
        fieldV(self.post_type) + \
        fieldV(self.promoted_eng_rate_by_reach) + \
        fieldV(self.promoted_impressions) + \
        fieldV(self.promoted_reach) + \
        fieldV(self.promoted_reach_rate) + \
        fieldV(self.promoted_video_views) + \
        fieldV(self.reach) + \
        fieldV(self.reach_rate) + \
        fieldV(self.share_count) + \
        fieldV(self.status_type) + \
        fieldV(self.timezone) + \
        fieldV(self.video_views) + \
        fieldV(self.media_url) + \
        self.period + """) on conflict(post_id, cliente_id, api_id, period) do update set """ + \
    """
      logproc_id = """ + self.logproc_id + """
    , logproc_api_id = """ + self.logproc_api_id + """
    , datahorainsert = """ + self.datahorainsert + """
    , author_id = """ + self.author_id + """
    , author_name = """ + self.author_name + """
    , author_picture_url = """ + self.author_picture_url + """
    , author_username = """ + self.author_username + """
    , comment_count = """ + self.comment_count + """
    , created_time = """ + self.created_time + """
    , eng_rate_by_reach = """ + self.eng_rate_by_reach + """
    , engagement_rate = """ + self.engagement_rate + """
    , fan_change_24h = """ + self.fan_change_24h + """
    , fans = """ + self.fans + """
    , impressions = """ + self.impressions + """
    , is_promoted = """ + self.is_promoted + """
    , like_count = """ + self.like_count + """
    , message = """ + self.message + """
    , organic_eng_rate_by_reach = """ + self.organic_eng_rate_by_reach + """
    , organic_impressions = """ + self.organic_impressions + """
    , organic_reach = """ + self.organic_reach + """
    , organic_reach_rate = """ + self.organic_reach_rate + """
    , organic_video_views = """ + self.organic_video_views + """
    , permalink_url = """ + self.permalink_url + """
    , post_type = """ + self.post_type + """
    , promoted_eng_rate_by_reach = """ + self.promoted_eng_rate_by_reach + """
    , promoted_impressions = """ + self.promoted_impressions + """
    , promoted_reach = """ + self.promoted_reach + """
    , promoted_reach_rate = """ + self.promoted_reach_rate + """
    , promoted_video_views = """ + self.promoted_video_views + """
    , reach = """ + self.reach + """
    , reach_rate = """ + self.reach_rate + """
    , share_count = """ + self.share_count + """
    , status_type = """ + self.status_type + """
    , timezone = """ + self.timezone + """
    , media_url = """ + self.media_url + """
    , video_views = """ + self.video_views + """;"""

    return query
