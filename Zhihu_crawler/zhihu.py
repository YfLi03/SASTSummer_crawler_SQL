import requests
import json
import pymysql
from bs4 import BeautifulSoup as BS
import logging
import time

fmt = '%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s'
datefmt = '%Y-%m-%d %H:%M:%S'
level = logging.INFO

formatter = logging.Formatter(fmt, datefmt)
logger = logging.getLogger()
logger.setLevel(level)

file = logging.FileHandler("../zhihu.log", encoding='utf-8')
file.setLevel(level)
file.setFormatter(formatter)
logger.addHandler(file)

console = logging.StreamHandler()
console.setLevel(level)
console.setFormatter(formatter)
logger.addHandler(console)


class ZhihuCrawler:
    """
    爬取知乎热榜信息
    采用了与提示中不同的接口, 获得信息可能有所出入
    """
    def __init__(self):
        with open("zhihu.json", "r", encoding="utf8") as f:
            self.settings = json.load(f)  # Load settings
        logger.info("Settings loaded")


    def sleep(self, sleep_key, delta=0):
        """
        Execute sleeping for a time configured in the settings

        :param sleep_key: the sleep time label
        :param delta: added to the sleep time
        :return:
        """
        _t = self.settings["config"][sleep_key] + delta
        logger.info(f"Sleep {_t} second(s)")
        time.sleep(_t)

    def query(self, sql, args=None, op=None):
        """
        Execute an SQL query

        :param sql: the SQL query to execute
        :param args: the arguments in the query
        :param op: the operation to cursor after query
        :return: op(cur)
        """
        conn = pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor,
            client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
            **self.settings['mysql']
        )
        if args and not (isinstance(args, tuple) or isinstance(args, list)):
            args = (args,)
        with conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(sql, args)
                    conn.commit()
                    if op is not None:
                        return op(cur)
                except:  # Log query then exit
                    if hasattr(cur, "_last_executed"):
                        logger.error("Exception @ " + cur._last_executed)
                    else:
                        logger.error("Exception @ " + sql)
                    raise

    def watch(self, top=None):
        """
        The crawling flow

        :param top: only look at the first `top` entries in the board. It can be used when debugging
        :return:
        """
        self.create_table()
        while True:
            logger.info("Begin crawling ...")
            try:
                crawl_id = None
                begin_time = time.time()
                crawl_id = self.begin_crawl(begin_time)

                try:
                    board_entries = self.get_board()
                except RuntimeError as e:
                    if isinstance(e.args[0], requests.Response):
                        logger.exception(e.args[0].status_code, e.args[0].text)
                    raise
                else:
                    logger.info(
                        f"Get {len(board_entries)} items: {','.join(map(lambda x: x['title'][:20], board_entries))}")
                if top:
                    board_entries = board_entries[:top]

                # Process each entry in the hot list
                for idx, item in enumerate(board_entries):
                    if item["qid"] is None:
                        logger.warning(f"Unparsed URL @ {item['url']} ranking {idx} in crawl {crawl_id}.")
                    try:
                        self.add_entry(crawl_id, idx, item)
                    except Exception as e:
                        logger.exception(f"Exception when adding entry {e}")
                self.end_crawl(crawl_id)
            except Exception as e:
                logger.exception(f"Crawl {crawl_id} encountered an exception {e}. This crawl stopped.")
            self.sleep("interval_between_board", delta=(begin_time - time.time()))

    def create_table(self):
        """
        Create tables to store the hot question records and crawl records

        """
        sql = f"""
CREATE TABLE IF NOT EXISTS `crawl` (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `begin` DOUBLE NOT NULL,
    `end` DOUBLE,
    PRIMARY KEY (`id`) USING BTREE
)
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `record`  (
    `id` BIGINT NOT NULL AUTO_INCREMENT,
    `qid` INT NOT NULL,
    `crawl_id` BIGINT NOT NULL,
    `hit_at` DOUBLE,
    `ranking` INT NOT NULL,
    `title` VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL ,
    `heat` VARCHAR(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL,
    `created` INT,
    `visitCount` INT,
    `followerCount` INT,
    `answerCount` INT,
    `raw` LONGTEXT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci ,
    `url` VARCHAR(255),
    PRIMARY KEY (`id`) USING BTREE,
    INDEX `CrawlAssociation` (`crawl_id`) USING BTREE,
    CONSTRAINT `CrawlAssociationFK` FOREIGN KEY (`crawl_id`) REFERENCES `crawl` (`id`)
) 
AUTO_INCREMENT = 1 
CHARACTER SET = utf8mb4 
COLLATE = utf8mb4_unicode_ci;

"""
        self.query(sql)

    def begin_crawl(self, begin_time) -> (int, float):
        """
        Mark the beginning of a crawl
        :param begin_time:
        :return: (Crawl ID, the time marked when crawl begin)
        """
        sql = """
INSERT INTO crawl (begin) VALUES(%s);
"""
        return self.query(sql, begin_time, lambda x: x.lastrowid)

    def end_crawl(self, crawl_id: int):
        """
        Mark the ending time of a crawl

        :param crawl_id: Crawl ID
        """
        sql = """
UPDATE crawl SET end = %s WHERE id = %s;
"""
        self.query(sql, (time.time(), crawl_id))

    def add_entry(self, crawl_id, idx, board):
        """
        Add a question entry to database

        :param crawl_id: Crawl ID
        :param idx: Ranking in the board
        :param board: dict, info from the board
        :param detail: dict, info from the detail page
        """
        sql = \
            """
INSERT INTO record (`qid`, `crawl_id`, `title`, `heat`, `created`, `visitCount`, `followerCount`, `answerCount`, `raw`, `ranking`, `hit_at`, `url`)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,  %s, %s, %s);
"""
        self.query(
            sql,
            (
                board["qid"],
                crawl_id,
                board["title"],
                board["heat"],
                board["created"],
                board["visitCount"],
                board["followerCount"],
                board["answerCount"],
                board["raw"],
                idx,
                board["hit_at"],
                board["url"]
            )
        )

    def get_board(self) -> list:
        """
        TODO: Fetch current hot questions, And **also the detailed info**

        :return: hot question list, ranking from high to low

        Return Example:
        [

            {
                'title':    # 问题标题
                'heat':     # 问题热度
                'url':      # 问题网址
                'qid':      # 问题编号
                "created": 1657248657,      # 问题的创建时间
                "followerCount": 5980,      # 问题的关注数量
                "visitCount": 2139067,      # 问题的浏览次数
                "answerCount": 2512         # 问题的回答数量
                "title": "日本前首相安倍      # 问题的标题
                    晋三胸部中枪已无生命
                    体征 ，嫌疑人被控制，
                    目前最新进展如何？背
                    后原因为何？",
                "raw": "<p>据央视新闻，        # 问题的详细描述
                    当地时间8日，日本前
                    首相安倍晋三当天上午
                    在奈良发表演讲时中枪
                    。据悉，安倍晋三在上
                    救护车时还有意。。。",
                "hit_at": 1657264954.3134503  # 请求的时间戳
            }
            ...
        ]
        """

        # Hint: - using a differnt origin , not the one provided in hint
        # Cannot get Expert from this origin
        try:
            resp = requests.get("https://www.zhihu.com/api/v4/creators/rank/hot?domain=0&period=hour", headers = self.settings["headers"])
            questions = json.loads(resp.text)["data"]
        except Exception as e:
            logger.exception(f"Cannot get the board ")
            return 
        
        
        fmt_questions = []

        for data in questions:
            question = data["question"]
            reaction = data["reaction"]

            #excerpt info is not included in the data, so the item is delted
            hit_at = time.time()

            try:
                resp = requests.get(question["url"], headers = self.settings["headers"])
                soup = BS(resp.text,"lxml")
                excerpt_father = soup.find_all(class_="QuestionRichText QuestionRichText--expandable QuestionRichText--collapsed")
                if len(excerpt_father) == 0:
                    excerpt = "None"
                else:
                    excerpt = excerpt_father[0].contents[0].contents[0].text
            except Exception as e:
                logger.exception(f"Cannot get the raw info of question " + question['title'])

            try:
                fmt_question = {
                    "title": question['title'],
                    "heat": reaction['new_pv_yesterday'],   #这一项似乎就是热度数据
                    "url": question["url"],
                    "qid": question["id"],
                    "created": question["created"],
                    "followerCount": reaction["follow_num"],
                    "visitCount": reaction["pv"],
                    "answerCount": reaction["answer_num"],
                    "raw": excerpt,
                    "hit_at": hit_at                          
                }
            except Exception as e:
                logger.exception(f"Cannot format the data of question " + question['title'])
    
            fmt_questions.append(fmt_question)
            logger.info(f"Get question detail for {question['title']}")
            self.sleep("interval_between_question")

        return fmt_questions



if __name__ == "__main__":
    print("""
    写完爬虫才发现爬的似乎不是真正的知乎热榜
    而是发现里面的"热点榜单"
    由于作者平时看的都是这个,所以爬"错"了
    不过该练习的都练到了..
    """)
    z = ZhihuCrawler()
    z.watch()
