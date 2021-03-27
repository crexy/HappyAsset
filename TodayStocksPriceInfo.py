from time import sleep

import selenium
from selenium import webdriver
from selenium.webdriver import ActionChains

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

class TodayStockPrice:





    def transFileEncoding(self, filepath, read_encoding, write_encoding):
        try:
            s = open(filepath, mode='r', encoding=read_encoding).read()
            open(filepath, mode='w', encoding=write_encoding).write(s)
        except:
            print('')


    def test1(self):
        driver = webdriver.Chrome(executable_path='./resources/webdriver/chromedriver')
        driver.get(url='https://www.google.com/')
        try:
            # EC: expected_conditions
            element = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CLASS_NAME, 'gLFyf')) #웹페이지에서 class가 gLFyf인 어떤 element를 찾을 수 있는지를 최대 5초 동안 매 0.5초마다 시도한다.
            )
        finally:
            driver.quit()

    def test2(self):
        options = webdriver.ChromeOptions()
        options.add_argument('window-size=1920,1080')

        driver = webdriver.Chrome(executable_path='./resources/webdriver/chromedriver', options=options)
        # 찾으려는 element가 로드될 때까지 지정한 시간만큼 대기할 수 있도록 설정한다.
        # 한 webdriver에 영구적으로 작용한다. 인자는 초 단위.
        driver.implicitly_wait(5)

        driver.get(url='https://www.google.com/')

        # Xpath로 엘리먼트 찾기
        search_box = driver.find_element_by_xpath('/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input')

        # 키입력
        search_box.send_keys('greeksharifa.github.io')
        search_box.send_keys(Keys.RETURN)

        #elements = driver.find_elements_by_xpath('//*[@id="rso"]/div/div[1]/div/div/div[1]/a/h3')
        # for element in elements:
        #     print(element.text)
        #     print(element.text, file=open('gorio.txt', 'w', encoding='utf-8'))

        posting = driver.find_element_by_xpath('//*[@id="rso"]/div/div[1]/div/div/div[1]/a/h3')
        # 클릭이벤트
        posting.click()

        # select 내에서 인덱스로 선택하거나, 옵션의 텍스트, 혹은 어떤 값을 통해 선택이 가능하다.
        # select = Select(driver.find_element_by_name('select_name'))
        # select.select_by_index(index=2)
        # select.select_by_visible_text(text="option_text")
        # select.select_by_value(value='고리오')

        # 선택을 해제하려면 다음 코드를 사용한다.
        # select.deselect_by_index(index=2)
        # select.deselect_by_visible_text(text="option_text")
        # select.deselect_by_value(value='고리오')
        # 전부 해제
        #select.deselect_all()
        
        # 선택된 옵션 리스트를 얻으려면 select.all_selected_options으로 얻을 수 있고, 
        # 첫 번째 선택된 옵션은 select.first_selected_option, 
        # 가능한 옵션을 모두 보려면 select.options를 사용하면 된다.        

        # Drag and Drop
        # action_chains = ActionChains(driver)
        # action_chains.drag_and_drop(source, target).perform()


        # Window / Frame 이동

        #  frame 안에 들어 있는 요소는 find_element 함수를 써도 그냥 찾아지지 않는다. find_element 함수는 frame 내에 있는 요소를 찾아주지 못한다.
        # 그래서 특정 frame으로 이동해야 할 때가 있다.
        # driver.switch_to_frame("frameName")
        # driver.switch_to_window("windowName")
        # # frame 내 subframe으로도 접근이 가능하다. 점(.)을 쓰자.
        # driver.switch_to_frame("frameName.0.child")

        # windowName을 알고 싶다면 다음과 같은 링크가 있는지 살펴보자.
        # <a href="somewhere.html" target="windowName">Click here to open a new window</a>

        # webdriver는 window 목록에 접근할 수 있기 때문에, 다음과 같이 모든 window를 순회하는 것도 가능하다.
        # for handle in driver.window_handles:
        #     driver.switch_to_window(handle)
        # frame 밖으로 나가려면 다음과 같이 쓰면 기본 frame으로 되돌아간다.
        # driver.switch_to_default_content()
        # 경고창으로 이동할 수도 있다.
        # alert = driver.switch_to.alert

        sleep(3)
        driver.close()

todayPrice = TodayStockPrice()

if __name__ == "__main__":
    todayPrice.getTodayStockPriceInfo()