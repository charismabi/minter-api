from lib import lib, libObj, libDb

class Tweets:
  id = ''
  cliente_id = ''
  api_id = ''
  logproc_api_id = ''
  datahorainsert = ''
  status_id = ''
  created_time = ''
  engagement_rate = ''
  favorite_count = ''
  follower_change_24h = ''
  followers = ''
  full_view_rate = ''
  impressions = ''
  is_promoted = ''
  is_quote_status = ''
  lang = ''
  link = ''
  organic_engagement_rate = ''
  organic_favorite_count = ''
  organic_full_view_rate = ''
  organic_impressions = ''
  organic_reply_count = ''
  organic_retweet_count = ''
  organic_url_link_clicks = ''
  organic_user_profile_clicks = ''
  organic_video_views = ''
  promoted_engagement_rate = ''
  promoted_favorite_count = ''
  promoted_full_view_rate = ''
  promoted_impressions = ''
  promoted_reply_count = ''
  promoted_retweet_count = ''
  promoted_url_link_clicks = ''
  promoted_user_profile_clicks = ''
  promoted_video_views = ''
  quote_count = ''
  reply_count = ''
  retweet_count = ''
  text = ''
  timezone = ''
  url_link_clicks = ''
  user_name = ''
  user_profile_picture = ''
  user_screen_name = ''
  user_profile_clicks = ''
  video_views = ''
  media_url = ''

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

    self.cliente_id = lib.valSql(cliApi.cli_id)
    self.api_id = lib.valSql(cliApi.api_id)
    self.datahorainsert = 'now()'
    self.logproc_api_id = str(logproc_api_id)

    self.status_id = lib.strSql(report_get('status_id'))
    self.created_time = lib.dataTimeToTimeStampSql(report_get('created_time'))
    self.engagement_rate = valSql('engagement_rate')
    self.favorite_count = valSql('favorite_count')
    self.follower_change_24h = valSql('follower_change_24h')
    self.followers = valSql('followers')
    self.full_view_rate = valSql('full_view_rate')
    self.impressions = valSql('impressions')
    self.is_promoted = lib.boolSql(report_get('is_promoted'))
    self.is_quote_status = lib.boolSql(report_get('is_quote_status'))
    self.lang = lib.strSql(report_get('lang'))
    self.link = lib.strSql(report_get('link'))

    self.organic_engagement_rate = valSql2('organic', 'engagement_rate')
    self.organic_favorite_count = valSql2('organic', 'favorite_count')
    self.organic_full_view_rate = valSql2('organic', 'full_view_rate')
    self.organic_impressions = valSql2('organic', 'impressions')
    self.organic_reply_count = valSql2('organic', 'reply_count')
    self.organic_retweet_count = valSql2('organic', 'retweet_count')
    self.organic_url_link_clicks = valSql2('organic', 'url_link_clicks')
    self.organic_user_profile_clicks = valSql2('organic', 'user_profile_clicks')
    self.organic_video_views = valSql2('organic', 'video_views')

    self.promoted_engagement_rate = valSql2('promoted', 'engagement_rate')
    self.promoted_favorite_count = valSql2('promoted', 'favorite_count')
    self.promoted_full_view_rate = valSql2('promoted', 'full_view_rate')
    self.promoted_impressions = valSql2('promoted', 'impressions')
    self.promoted_reply_count = valSql2('promoted', 'reply_count')
    self.promoted_retweet_count = valSql2('promoted', 'retweet_count')
    self.promoted_url_link_clicks = valSql2('promoted', 'url_link_clicks')
    self.promoted_user_profile_clicks = valSql2('promoted', 'user_profile_clicks')
    self.promoted_video_views = valSql2('promoted', 'video_views')

    self.quote_count = valSql('quote_count')
    self.reply_count = valSql('reply_count')
    self.retweet_count = valSql('retweet_count')
    self.text = lib.strSql(report_get('text'))
    self.timezone = valSql('timezone')
    self.url_link_clicks = valSql('url_link_clicks')
    self.user_name = lib.strSql(report_get2('user', 'name'))
    self.user_profile_picture = lib.strSql(report_get2('user', 'profile_picture'))
    self.user_screen_name = lib.strSql(report_get2('user', 'screen_name'))
    self.user_profile_clicks = valSql('user_profile_clicks')
    self.video_views = valSql('video_views')
    self.media_url = 'null'

    if 'entities' in json:
      if 'media' in json['entities']:
        if len(json['entities']['media']) > 0:
          self.media_url = lib.strSql(json['entities']['media'][0]['media_url'])

  def getInsert(self):
    def fieldV(value):
      return str(value) + ', '

    query = """
          INSERT INTO minter.tweets
          (cliente_id, api_id, logproc_api_id, datahorainsert, status_id, created_time, engagement_rate, favorite_count, follower_change_24h, followers, full_view_rate, impressions, is_promoted, is_quote_status, lang, link, organic_engagement_rate, organic_favorite_count, organic_full_view_rate, organic_impressions, organic_reply_count, organic_retweet_count, organic_url_link_clicks, organic_user_profile_clicks, organic_video_views, promoted_engagement_rate, promoted_favorite_count, promoted_full_view_rate, promoted_impressions, promoted_reply_count, promoted_retweet_count, promoted_url_link_clicks, promoted_user_profile_clicks, promoted_video_views, quote_count, reply_count, retweet_count, "text", timezone, url_link_clicks, user_name, user_profile_picture, user_screen_name, user_profile_clicks, media_url, video_views) values (""" + \
        fieldV(self.cliente_id) + \
        fieldV(self.api_id) + \
        fieldV(self.logproc_api_id) + \
        fieldV(self.datahorainsert) + \
        fieldV(self.status_id) + \
        fieldV(self.created_time) + \
        fieldV(self.engagement_rate) + \
        fieldV(self.favorite_count) + \
        fieldV(self.follower_change_24h) + \
        fieldV(self.followers) + \
        fieldV(self.full_view_rate) + \
        fieldV(self.impressions) + \
        fieldV(self.is_promoted) + \
        fieldV(self.is_quote_status) + \
        fieldV(self.lang) + \
        fieldV(self.link) + \
        fieldV(self.organic_engagement_rate) + \
        fieldV(self.organic_favorite_count) + \
        fieldV(self.organic_full_view_rate) + \
        fieldV(self.organic_impressions) + \
        fieldV(self.organic_reply_count) + \
        fieldV(self.organic_retweet_count) + \
        fieldV(self.organic_url_link_clicks) + \
        fieldV(self.organic_user_profile_clicks) + \
        fieldV(self.organic_video_views) + \
        fieldV(self.promoted_engagement_rate) + \
        fieldV(self.promoted_favorite_count) + \
        fieldV(self.promoted_full_view_rate) + \
        fieldV(self.promoted_impressions) + \
        fieldV(self.promoted_reply_count) + \
        fieldV(self.promoted_retweet_count) + \
        fieldV(self.promoted_url_link_clicks) + \
        fieldV(self.promoted_user_profile_clicks) + \
        fieldV(self.promoted_video_views) + \
        fieldV(self.quote_count) + \
        fieldV(self.reply_count) + \
        fieldV(self.retweet_count) + \
        fieldV(self.text) + \
        fieldV(self.timezone) + \
        fieldV(self.url_link_clicks) + \
        fieldV(self.user_name) + \
        fieldV(self.user_profile_picture) + \
        fieldV(self.user_screen_name) + \
        fieldV(self.user_profile_clicks) + \
        fieldV(self.media_url) + \
        self.video_views + """) on conflict(status_id, cliente_id, api_id) do update set """ + \
    """  
      engagement_rate = """ + self.engagement_rate + """
    , favorite_count = """ + self.favorite_count + """
    , follower_change_24h = """ + self.follower_change_24h + """
    , followers = """ + self.followers + """
    , full_view_rate = """ + self.full_view_rate + """
    , impressions = """ + self.impressions + """
    , is_promoted = """ + self.is_promoted + """
    , is_quote_status = """ + self.is_quote_status + """
    , lang = """ + self.lang + """
    , link = """ + self.link + """
    , organic_engagement_rate = """ + self.organic_engagement_rate + """
    , organic_favorite_count = """ + self.organic_favorite_count + """
    , organic_full_view_rate = """ + self.organic_full_view_rate + """
    , organic_impressions = """ + self.organic_impressions + """
    , organic_reply_count = """ + self.organic_reply_count + """
    , organic_retweet_count = """ + self.organic_retweet_count + """
    , organic_url_link_clicks = """ + self.organic_url_link_clicks + """
    , organic_user_profile_clicks = """ + self.organic_user_profile_clicks + """
    , organic_video_views = """ + self.organic_video_views + """
    , promoted_engagement_rate = """ + self.promoted_engagement_rate + """
    , promoted_favorite_count = """ + self.promoted_favorite_count + """
    , promoted_full_view_rate = """ + self.promoted_full_view_rate + """
    , promoted_impressions = """ + self.promoted_impressions + """
    , promoted_reply_count = """ + self.promoted_reply_count + """
    , promoted_retweet_count = """ + self.promoted_retweet_count + """
    , promoted_url_link_clicks = """ + self.promoted_url_link_clicks + """
    , promoted_user_profile_clicks = """ + self.promoted_user_profile_clicks + """
    , promoted_video_views = """ + self.promoted_video_views + """
    , quote_count = """ + self.quote_count + """
    , reply_count = """ + self.reply_count + """
    , retweet_count = """ + self.retweet_count + """
    , text = """ + self.text + """
    , timezone = """ + self.timezone + """
    , url_link_clicks = """ + self.url_link_clicks + """
    , user_name = """ + self.user_name + """
    , user_profile_picture = """ + self.user_profile_picture + """
    , user_screen_name = """ + self.user_screen_name + """
    , media_url = """ + self.media_url + """
    , user_profile_clicks = """ + self.user_profile_clicks + """;"""

    return query
