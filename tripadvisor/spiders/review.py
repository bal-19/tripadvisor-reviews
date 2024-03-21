from datetime import datetime
from pathlib import Path
from io import BytesIO
from uuid import uuid4
from PIL import Image
import logging
import locale
import scrapy
import base64
import s3fs
import json
import re
import os


class ReviewSpider(scrapy.Spider):
    name = "review"
    # ==================================================================================================================================
    start_urls = ["https://www.tripadvisor.co.id/Restaurant_Review-g1493703-d19764029-Reviews-Sixth_Sense-Tangerang_Banten_Province_Java.html"]
    # ==================================================================================================================================
    
    # ====================================================================
    custom_settings = {
        'USER_AGENT' : 'YOUR_USER_AGENT'
    }
    # ====================================================================
    
    total_success = 0
    total_failed = 0

    def request(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse)
            
    def upload_to_s3(self, rpath, lpath):        
        client_kwargs = {
            'key': 'YOUR_S3_KEY',
            'secret': 'YOUR_S3_SECRET',
            'endpoint_url': 'YOUR_S3_ENDPOINT',
            'anon': False
        }

        s3 = s3fs.core.S3FileSystem(**client_kwargs)

        # Upload file
        s3.upload(rpath=rpath, lpath=lpath)
    
    def log_error(self, crawling_time, id_project, project, sub_project, source_name, sub_source_name, id_sub_source, id_data, process_name, status, type_error, message, assign, path):
        log_error = {
            "crawlling_time": crawling_time,
            "id_project": id_project,
            "project": project,
            "sub_project": sub_project,
            "source_name": source_name,
            "sub_source_name": sub_source_name,
            "id_sub_source": id_sub_source,
            "id_data": id_data,
            "process_name": process_name,
            "status": status,
            "type_error": type_error,
            "message": message,
            "assign": assign
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log_error)

        with open(path, 'w') as file:
            json.dump(existing_data, file)
            
            
    def log(self, crawling_time, id_project, project, sub_project, source_name, sub_source, id_sub_source, total, total_success, total_failed, status, assign, path):
        log = {
            'crawling_time': crawling_time,
            'id_project': id_project,
            'project': project,
            'sub_project': sub_project,
            'source_name': source_name,
            'sub_source_name': sub_source,
            'id_sub_source': id_sub_source,
            'total_data': int(total),
            'total_success': total_success,
            'total_failed': total_failed,
            'status': status,
            'assign': assign,
        }
        
        try:
            with open(path, 'r') as file:
                existing_data = json.load(file)
        except FileNotFoundError:
            existing_data = []

        existing_data.append(log)

        with open(path, 'w') as file:
            json.dump(existing_data, file)

    def parse(self, response):
        url = response.url
        domain = url.split('/')[2]
        sub_source = response.xpath('//*[@id="taplc_trip_planner_breadcrumbs_0"]/ul/li/a/span/text()').getall()
        sub_source = sub_source[-1]
        id_sub_src = int(str(uuid4()).replace('-', ''), 16)
        # ==============================================================
        category_reviews = 'food & baverage'
        # ==============================================================
        crawling_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        crawling_time_epoch = int(datetime.now().timestamp() * 1000)
        # =======================================================================================================================
        # YOUR S3 PATH
        path_data_raw = f's3://{domain}/{sub_source.replace(' ', '_')}/json'
        path_data_clean = f's3://{domain}/{sub_source.replace(' ', '_')}/json'
        # =======================================================================================================================
        
        # logging
        id_project = None
        project = 'data review'
        sub_project = 'data review'
        assign = 'iqbal'

        # resto information
        resto = response.css('div.lBkqB._T > div.acKDw.w.O > h1::text').get()
        resto_location = response.css('div.lBkqB._T > div:nth-child(3) > span:nth-child(1) > span > a::text').get()
        total_reviews = response.css('div.lBkqB._T > div:nth-child(2) > span:nth-child(1) > a > span::text').get().replace('ulasan', '').replace('.', '')
        total_rating = response.css('div.lBkqB._T > div:nth-child(2) > span:nth-child(1) > a > svg::attr(aria-label)').get().replace(' dari 5 lingkaran', '').replace(',', '')
        total_rating = float(total_rating)/10
        peringkat = []
        for rank in response.css('div.hILIJ > div > div:nth-child(1) > div > div:nth-child(1) > div.cNFlb'):
            desc = rank.css('a::text').get()
            nomor = rank.css('b > span::text').get()
            dari = rank.css('div.cNFlb::text').get()
            ranking = nomor + dari + desc
            peringkat.append(ranking)
        resto_telp = response.css('div.lBkqB._T > div:nth-child(3) > span:nth-child(2) > span > span.AYHFM > a::text').get()
        
        # ratings restoran
        detail_total_rating = []
        for ratings in response.css('div.hILIJ > div > div:nth-child(1) > div > div:nth-child(3) > div:nth-child(2) > div.DzMcu'):
            category_rating = ratings.css('span.BPsyj::text').get()
            rating = ratings.css('span.vzATR > span::attr(class)').get()
            rating = rating.split(' ')[-1].replace('bubble_', '')
            rating = float(rating)/10
            
            detail_total_rating.append({
                'score_rating' : rating,
                'category_rating' : category_rating
            })
            
        # rincian restoran
        rincian = []
        for rinci in response.css('div.hILIJ > div > div:nth-child(2) > div > div > div.BMlpu > div'):
            title_rincian = rinci.css('div.tbUiL.b::text').get()
            desc_rincian = rinci.css('div.SrqKb::text').get()
            
            rincian.append({
                'title' : title_rincian,
                'description' : desc_rincian
            })
            
        # scraping reviews
        for review in response.css('div.review-container'):
            try:
                id_review = review.css('div.review-container::attr(data-reviewid)').get()
                if id_review is not None:
                    review_nickname = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-2 > div > div > div:nth-child(1) > div.info_text > div::text').get()
                    
                    review_avatar = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-2 > div > div > div:nth-child(1) > div.avatar > div  > a > div > div > img').xpath('@src').get()
                    # image_bytes = base64.b64decode(review_avatar.split(',')[-1])
                    # with open(f'{review_nickname}.jpg', 'wb') as image_file:
                    #     image_file.write(image_bytes)
                        
                    rating_user = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > span.ui_bubble_rating::attr(class)').get()
                    rating_user = rating_user.split(' ')[-1].replace('bubble_', '')
                    rating_user = float(rating_user)/10
                    review_title = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > div.quote > a.title > span.noQuotes::text').get()
                    review_content = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > div.prw_rup.prw_reviews_text_summary_hsx > div > p::text').get()
                    review_like = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > div.prw_rup.prw_reviews_vote_line_hsx > div.helpful.redesigned.hsx_helpful > span > span > span.numHelp::text').get()
                    if review_like is None:
                        review_like = 0
                    else:
                        review_like = int(review_like.replace('\u00a0', ''))
                    review_date = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > span.ratingDate::attr(title)').get()
                    date_of_exp = review.css(f'div > div#review_{id_review} > div > div.ui_column.is-9 > div.prw_rup.prw_reviews_stay_date_hsx::text').get()
                    locale.setlocale(locale.LC_TIME, 'id_ID')
                    review_date = datetime.strptime(review_date, '%d %B %Y')
                    if date_of_exp is not None:
                        date_of_exp = datetime.strptime(date_of_exp, ' %B %Y')
                    locale.setlocale(locale.LC_TIME, 'en_US')
                    review_date = review_date.strftime('%Y-%m-%d %H:%M:%S')
                    review_date_epoch = int(datetime.strptime(review_date, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                    if date_of_exp is not None:
                        date_of_exp = date_of_exp.strftime('%Y-%m-%d %H:%M:%S')
                        date_of_exp_epoch = int(datetime.strptime(date_of_exp, '%Y-%m-%d %H:%M:%S').timestamp() * 1000)
                    else: 
                        date_of_exp = review_date
                        date_of_exp_epoch = review_date_epoch
                    
                    file_name = f'{resto.replace(" ", "_")}_{id_review}_{review_date_epoch}.json'
                    
                    resto_info = {
                        'link' : url,
                        'domain' : domain,
                        'tag' : [domain, category_reviews, resto],
                        'crawling_time' : crawling_time,
                        'crawling_time_epoch' : crawling_time_epoch,
                        'path_data_raw' : f'{path_data_raw}/{file_name}',
                        'path_data_clean' : f'{path_data_clean}/{file_name}',
                        'reviews_name' : resto,
                        'location_reviews' : resto_location,
                        'telp_reviews' : resto_telp,
                        'category_reviews' : category_reviews,
                        'total_reviews' : int(total_reviews),
                        'rincian' : rincian,
                        'rank' : peringkat,
                        'reviews_rating' : {
                            'total_rating' : total_rating,
                            'detail_total_rating' : detail_total_rating
                        }
                    }
                    
                    reviews_info = {
                        'detail_reviews' : {
                            'id_review' : id_review,
                            'username_reviews' : review_nickname,
                            'image_reviews' : review_avatar,
                            'created_time' : review_date,
                            'created_time_epoch' : review_date_epoch,
                            'email_reviews' : None,
                            'company_name' : resto,
                            'title_detail_reviews' : review_title,
                            'reviews_rating' : rating_user,
                            'detail_reviews_rating' : [],
                            'total_likes_reviews' : review_like,
                            'total_dislikes_reviews' : None,
                            'content_reviews' : review_content,
                            'reply_content_reviews' : [],
                            'date_of_experience' : date_of_exp,
                            'date_of_experience_epoch' : date_of_exp_epoch 
                                
                        }
                    }
                    
                    data = {**resto_info, **reviews_info}
                    
                    # saving to json
                    # ==============================================================================================================
                    # YOUR LOCAL PATH
                    folder_name = 'F:/Work/Crawling Tripadvisor/data'
                    # ==============================================================================================================
                    if not os.path.exists(folder_name):
                        os.makedirs(folder_name)
                    with open(f'{folder_name}/{file_name}', 'w') as f:
                        json.dump(data, f)
                    
                    # upload to s3
                    # ==============================================================================================================
                    # self.upload_to_s3(f'{path_data_raw.replace('s3://', '')}/{file_name}', f'{folder_name}/{file_name}') 
                    # ==============================================================================================================
                    # uncomment please if you want upload to s3
                    
                    self.total_success += 1
                    self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, id_review, 'crawling', 'success', '', '' , assign, 'log_error.json')
            except Exception as e:
                self.total_failed += 1
                self.log_error(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, id_review, 'crawling', 'error', str(e), type(e).__name__, assign, 'log_error.json')
                
        # pagination
        next_page = response.css('#taplc_location_reviews_list_resp_rr_resp_0 > div > div > div > div > a.nav.next::attr(href)').get()
        if next_page is not None:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse)
        else:
            total_data = self.total_success + self.total_failed
            self.log(crawling_time, id_project, project, sub_project, domain, sub_source, id_sub_src, total_data, self.total_success, self.total_failed, 'done', assign, 'log.json')