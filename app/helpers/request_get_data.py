import requests
import time
import json
from ..schemas.search_schemas import CrawlerPostItem
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
def formatTimestamp(timestamp):
        datetime = time.strftime(DATETIME_FORMAT, time.localtime(int(timestamp)))
        return datetime
def get_data_post_hastag_ig_recent(res,keyword):
        data=res['node']
        try:
            post_id=data['shortcode']
            url_post="https://www.instagram.com/p/"+post_id
        except:
            post_id=''
            url_post=''
        try:
            message=data['edge_media_to_caption']['edges'][0]['node']['text']
        except:
            message=''
        try:
            post_image=data['display_url']
        except:
            post_image=''
        try:
            post_created_timestamp=data['taken_at_timestamp']
            post_created=formatTimestamp(post_created_timestamp)
        except:
            post_created=formatTimestamp(1748937706)
            post_created_timestamp=1748937706
        try:
            like_count=data['edge_liked_by']['count']
        except:
            like_count=0
        try:
            comment_count=data['edge_media_to_comment']["count"]
        except:
            comment_count=0
        try:
            author_id=data['owner']['id']
        except:
            author_id=''
        try:
            full_name=data['caption']['user']['full_name']
        except:
            full_name=''
        try:
            author_username=data['caption']['user']['username']
        except:
            author_username=''
        try:
            author_image=data['caption']['user']['profile_pic_url']
        except:
            author_image=''
        #Object Item
        item = CrawlerPostItem(
            post_id=post_id,
            post_type="instagram",
            post_keyword=keyword,
            post_url=url_post,
            message=message,
            type=0,
            post_image=post_image,
            post_created=post_created,
            post_created_timestamp=post_created_timestamp,
            post_raw="",
            count_like=like_count,
            count_share=0,
            count_comments=comment_count,
            comments="",
            brand_id="",
            object_id="",
            service_id="",
            parent_post_id="",
            parent_object_id="",
            parent_service_id="",
            page_id="",
            page_name="",
            author_id=author_id,
            author_name=full_name,
            author_username=author_username,
            author_image=author_image,
            data_form_source=0,
        )
        return item

async def get_request_data_instagram( keyword: str )  -> list[CrawlerPostItem]:
    """
    Fetch data from a given URL with optional parameters.
    
    :keyword: The keyword to fetch data from instagram.
    :return: Response object containing the fetched data.
    """
    
    try:
        headers = {
            'accept': '*/*',
            'accept-language': 'vi,en-US;q=0.9,en;q=0.8,fr-FR;q=0.7,fr;q=0.6',
            'priority': 'u=1, i',
            'referer': 'https://www.instagram.com/explore/search/keyword/?q={keyword}',
            'sec-ch-prefers-color-scheme': 'dark',
            'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            'sec-ch-ua-full-version-list': '"Google Chrome";v="137.0.7151.124", "Chromium";v="137.0.7151.124", "Not/A)Brand";v="24.0.0.0"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-model': '""',
            'sec-ch-ua-platform': '"Windows"',
            'sec-ch-ua-platform-version': '"10.0.0"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-asbd-id': '359341',
            'x-csrftoken': '8wg6Fj5rDHqi0VwlBjF9e2nwnYLHvcQy',
            'x-ig-app-id': '936619743392459',
            'x-ig-www-claim': 'hmac.AR2FSYbLkevUBDCqMQn-xnlIjZst3cTUXSMNMmxhp-LuyH66',
            'x-requested-with': 'XMLHttpRequest',
            'x-web-session-id': 'wyh59a:g0lokc:09aoyj',
    
        }
        cookies = {
            "datr": "FH0uaE9jzr2zLPSntS6XVb6y",
            "ig_did": "009BE4D0-E24E-4C38-B68C-D4130764E64B",
            "mid": "aAhXYQALAAFFk406Xx_Zu60Pk67y",
            "ig_nrcb": "1",
            "fbm_124024574287414": "base_domain=.instagram.com",
            "csrftoken": "8wg6Fj5rDHqi0VwlBjF9e2nwnYLHvcQy",
            "ds_user_id": "74091417494",
            "ig_direct_region_hint": "\"ASH\\05458448736471\\0541779954390:01f756ac5f88a79ac1f70c32b2ce99c3895da8b76658d17d6b71cb9015d3c63ccfb638f0\"",
            "ps_l": "1",
            "ps_n": "1",
            "wd": "1260x932",
            "sessionid": "74091417494%3A7bOpE4Idabr0k0%3A2%3AAYeNdr51NvMepRewwKDwZfd6-vPNYX5AbDNtwBewsJ8",
            "rur": "VLL\\05474091417494\\0541784366622:01fe75b8de6615d5f1f1b19bdaede61f9a34a6bae4f78ba4ca404410668abdd30e22f1bb"
        }
        url_rq =f"https://www.instagram.com/graphql/query/?query_hash=9b498c08113f1e09617a1703c22b2f32&variables=%7B%22tag_name%22%3A%22{keyword}%22%2C%22first%22%3A100%2C%22after%22%3Anull%7D"
        print(url_rq)
        response = requests.get(url=url_rq, headers=headers, cookies=cookies)
        #response.raise_for_status()  # Raise an error for bad responses
        data_res = json.loads(response.text)
        #print(data_res)
        try:
            list_post=data_res['data']['hashtag']['edge_hashtag_to_media']['edges']
        except:
            list_post=[]
        if not list_post:
            return []
        list_post_res = []
        for post in list_post:       
            item = get_data_post_hastag_ig_recent(post,keyword)
            list_post_res.append(item)
        return list_post_res
    except requests.RequestException as e:
        raise Exception(f"Error fetching data from instagram: {str(e)}")