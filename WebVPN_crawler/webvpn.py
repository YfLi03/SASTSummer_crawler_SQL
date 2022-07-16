import time

from selenium.webdriver.remote.webdriver import WebDriver as wd
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait as wdw
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains as AC
import selenium
from bs4 import BeautifulSoup as BS
from IPython import embed
import json
import re


class WebVPN:
    def __init__(self, opt: dict, headless=False):
        self.root_handle = None
        self.driver: wd = None
        self.userid = opt["username"]
        self.passwd = opt["password"]
        self.headless = headless

    def login_webvpn(self):
        """
        Log in to WebVPN with the account specified in `self.userid` and `self.passwd`

        :return:
        """
        d = self.driver
        if d is not None:
            d.close()
        d = selenium.webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        d.get("https://webvpn.tsinghua.edu.cn/login")
        username = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item"]//input'
                                   )[0]
        password = d.find_elements(By.XPATH,
                                   '//div[@class="login-form-item password-field" and not(@id="captcha-wrap")]//input'
                                   )[0]
        username.send_keys(str(self.userid))
        password.send_keys(self.passwd)
        d.find_element(By.ID, "login").click()
        self.root_handle = d.current_window_handle
        self.driver = d
        return d

    def access(self, url_input):
        """
        Jump to the target URL in WebVPN

        :param url_input: target URL
        :return:
        """
        d = self.driver
        url = By.ID, "quick-access-input"
        btn = By.ID, "go"
        wdw(d, 5).until(EC.visibility_of_element_located(url))
        actions = AC(d)
        actions.move_to_element(d.find_element(*url))
        actions.click()
        actions. \
            key_down(Keys.CONTROL). \
            send_keys("A"). \
            key_up(Keys.CONTROL). \
            send_keys(Keys.DELETE). \
            perform()

        d.find_element(*url)
        d.find_element(*url).send_keys(url_input)
        d.find_element(*btn).click()

    def switch_another(self):
        """
        If there are only 2 windows handles, switch to the other one

        :return:
        """
        d = self.driver
        assert len(d.window_handles) == 2
        wdw(d, 5).until(EC.number_of_windows_to_be(2))
        for window_handle in d.window_handles:
            if window_handle != d.current_window_handle:
                d.switch_to.window(window_handle)
                return

    def to_root(self):
        """
        Switch to the home page of WebVPN

        :return:
        """
        self.driver.switch_to.window(self.root_handle)

    def close_all(self):
        """
        Close all window handles

        :return:
        """
        while True:
            try:
                l = len(self.driver.window_handles)
                if l == 0:
                    break
            except selenium.common.exceptions.InvalidSessionIdException:
                return
            self.driver.switch_to.window(self.driver.window_handles[0])
            self.driver.close()

    def login_info(self):
        """
		log in to info 
        :return:
        """
        self.access("info.tsinghua.edu.cn")
        self.switch_another()
        d = self.driver
        username = By.ID, "userName"
        # print(username)
        wdw(d, 10).until(EC.visibility_of_element_located(username))
        password = By.NAME, "password"
        # print(password)
        wdw(d, 10).until(EC.visibility_of_element_located(password))

        username = d.find_element(*username)
        password = d.find_element(*password)
        btn = d.find_element(By.CLASS_NAME, "but")
        btn = btn.find_element(By.TAG_NAME, "input")

        username.send_keys(str(self.userid))
        password.send_keys(self.passwd)
        btn.click()

        time.sleep(3)  # make sure you're logged in
        self.driver.close()


    def get_grades(self):
        """
        Get and calculate the GPA for each semester.
        Returns courses and GPAs
        
        Example print:
            2020-秋: *.**
            
            2021-春: *.**

        return:
        [{'course_name': '数字娱乐中的媒体技术', 'course_point': '1.0'}, {'course_name': '微积分A(1)', 'course_point': '2.3'}]
        """

        self.access("zhjw.cic.tsinghua.edu.cn/cj.cjCjbAll.do?m=bks_cjdcx&cjdlx=zw")
        time.sleep(1)
        self.switch_another()

        d = self.driver
        html = d.find_elements(By.TAG_NAME, "tbody")[3].get_attribute("innerHTML")
        soup = BS(html, "html.parser")
        courses = soup.find_all("tr")
        courses_info = []
        semesters = {}

        for course in courses[1:]:
            gpa = re.search("\d\.\d", course.find_all("td")[4].contents[0])
            if gpa is None:
                gpa = "N/A"
            else:
                gpa = gpa.group()

            semester = re.search("\\n.+\d.+\\n",course.find_all("td")[5].contents[0]).group()
            name = course.find_all("td")[1].contents[0]
            point = course.find_all("td")[2].contents[0]

            course_info = {
                "course_name": name,
                "course_grade": gpa
            }
            courses_info.append(course_info)

            if semester in semesters:
                semesters[semester].append([gpa, point])
            else:
                semesters[semester] = [[gpa, point]]


        for name, semester in semesters.items():
            point_sum = 0
            gpa_sum = 0.0
            for course in semester:
                if course[0] != "N/A":
                    gpa_sum += float(course[0])*int(course[1])
                    point_sum += int(course[1])
            print (name[:-1], ": ", round(gpa_sum/point_sum,2))

        #for course in courses_info:
        #    print(course["course_name"], ": ", course["course_grade"])

        return courses_info


if __name__ == "__main__":
    # TODO: Write your own query process
    with open("settings.json", "r", encoding="utf8") as f:
        settings = json.load(f)  # Load settings

    w = WebVPN(settings)
    w.login_webvpn()
    print("here")
    w.login_info()
    w.to_root()
    w.get_grades()
