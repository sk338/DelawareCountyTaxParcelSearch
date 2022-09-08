import requests
from bs4 import BeautifulSoup as soup
from datetime import datetime
import time
import os
import random
from queue import Queue
from threading import Thread

URL = 'http://delcorealestate.co.delaware.pa.us/pt/Search/Disclaimer.aspx?FromUrl=../search/commonsearch.aspx?mode=parid'
INPUT_FILE = 'ParcelNumbers.csv'
OUTPUT_FILE = f'DelawareCountyTaxDetails_{datetime.now().strftime("%m_%d_%Y")}.csv'


useragents = [account.rstrip() for account in open("useragents.txt").readlines()]
parcelnumbers = set([f.replace('\n', '').replace('-', '').strip() for f in open(INPUT_FILE).readlines()][1:])
try:
    already_done = set([done.strip() for done in open("progress.txt").read().split()])
except:
    already_done = set([])
parcelnumbers = set(parcelnumbers - already_done)

if not os.path.exists(OUTPUT_FILE):
    Output_Handle = open(OUTPUT_FILE, 'a')
    Output_Handle.write('ParcelID,Taxes2021,Taxes2020,Taxes2019,Taxes2018,Taxes2017,Taxes2016,Taxes2015,Mortgage Company,Mortgage.Service Co Name\n')
    Output_Handle.flush()
else:
    Output_Handle = open(OUTPUT_FILE, 'a')

Progress_Handle = open("progress.txt", 'a')
Exception_Handle = open("error.txt", 'a')

THREADS = 30

numbers_q = Queue()
for number in parcelnumbers:
    numbers_q.put(number)


class Check(Thread):
    def __init__(self, number_queue):
        self.number_q = number_queue
        Thread.__init__(self)

    def run(self):
        while not self.number_q.empty():
            parcel_number = self.number_q.get()
            Status = False
            try:
                s = requests.session()
                useragent = random.choice(useragents)
                s.headers.update({
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                    'Accept-Encoding': 'gzip, deflate',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Host': 'delcorealestate.co.delaware.pa.us',
                    'Pragma': 'no-cache',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': useragent
                })

                resp1 = s.get(URL, data={'FromUrl': '../search/commonsearch.aspx?mode=parid'})
                if resp1.status_code == 200:
                    resp1soup = soup(resp1.text, 'html.parser')
                    body = {
                        '__VIEWSTATE': resp1soup.find('input', {'id': '__VIEWSTATE'})['value'],
                        '__VIEWSTATEGENERATOR': resp1soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value'],
                        '__EVENTVALIDATION': resp1soup.find('input', {'id': '__EVENTVALIDATION'})['value'],
                        'btAgree': '',
                        'hdURL': '../search/commonsearch.aspx?mode=parid',
                        'action': ''
                    }

                    s.headers.update({
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'Accept-Encoding': 'gzip, deflate',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Cache-Control': 'no-cache',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Host': 'delcorealestate.co.delaware.pa.us',
                        'Origin': 'http://delcorealestate.co.delaware.pa.us',
                        'Pragma': 'no-cache',
                        'Referer': 'http://delcorealestate.co.delaware.pa.us/pt/Search/Disclaimer.aspx?FromUrl=../search/commonsearch.aspx?mode=parid',
                        'Upgrade-Insecure-Requests': '1',
                        'User-Agent': useragent
                    })
                    # resp2 = s.post('http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=parid', data=body, allow_redirects=True)
                    resp2 = s.post(
                        'http://delcorealestate.co.delaware.pa.us/pt/Search/Disclaimer.aspx?FromUrl=..%2fsearch%2fcommonsearch.aspx%3fmode%3dparid',
                        data=body, allow_redirects=True)
                    if resp2.status_code == 200:
                        # print(resp2.text)
                        resp2soup = soup(resp2.text, 'html.parser')
                        body1 = {
                            '__EVENTTARGET': '',
                            '__EVENTARGUMENT': '',
                            '__VIEWSTATE': resp2soup.find('input', {'id': '__VIEWSTATE'})['value'],
                            '__VIEWSTATEGENERATOR': resp2soup.find('input', {'id': '__VIEWSTATEGENERATOR'})['value'],
                            '__EVENTVALIDATION': resp2soup.find('input', {'id': '__EVENTVALIDATION'})['value'],
                            'PageNum': '',
                            'SortBy': 'PARID',
                            'SortDir': 'asc',
                            'PageSize': '15',
                            'hdAction': 'Search',
                            'hdIndex': '',
                            'sIndex': '-1',
                            'hdListType': 'PA',
                            'hdJur': '',
                            'hdSelectAllChecked': False,
                            'inpParid': parcel_number,
                            'selSortBy': 'PARID',
                            'selSortDir': 'asc',
                            'selPageSize': '15',
                            'searchOptions$hdBeta': '',
                            'btSearch': '',
                            'RadWindow_NavigateUrl_ClientState': '',
                            'mode': 'PARID',
                            'mask': '',
                            'param1': '',
                            'searchimmediate': ''
                        }
                        s.headers.update({
                            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                            'Accept-Encoding': 'gzip, deflate',
                            'Accept-Language': 'en-US,en;q=0.9',
                            'Cache-Control': 'no-cache',
                            'Connection': 'keep-alive',
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'Host': 'delcorealestate.co.delaware.pa.us',
                            'Origin': 'http://delcorealestate.co.delaware.pa.us',
                            'Pragma': 'no-cache',
                            'Referer': 'http://delcorealestate.co.delaware.pa.us/pt/search/commonsearch.aspx?mode=parid',
                            'Upgrade-Insecure-Requests': '1',
                            'User-Agent': useragent
                        })

                        resp3 = s.post(
                            'http://delcorealestate.co.delaware.pa.us/pt/search/CommonSearch.aspx?mode=PARID',
                            data=body1, allow_redirects=True)
                        if resp3.status_code == 200:
                            # print(resp3.status_code)
                            resp3soup = soup(resp3.text, 'html.parser')
                            MortgageCompany = []
                            MortgageService = []

                            try:
                                MortgageCompany.append(
                                    resp3soup.find(id='Mortgage Company').find_all('tr')[1].findAll('td')[0].text)
                                MortgageService.append(
                                    resp3soup.find(id='Mortgage Company').find_all('tr')[1].findAll('td')[1].text)
                            except Exception as e:
                                # print('No Mortgage')
                                MortgageCompany.append('')
                                MortgageService.append('')

                            s.headers.update({
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                                'Accept-Encoding': 'gzip, deflate',
                                'Accept-Language': 'en-US,en;q=0.9',
                                'Cache-Control': 'no-cache',
                                'Connection': 'keep-alive',
                                'Host': 'delcorealestate.co.delaware.pa.us',
                                'Pragma': 'no-cache',
                                'Referer': 'http://delcorealestate.co.delaware.pa.us/pt/Datalets/Datalet.aspx?sIndex=0&idx=1',
                                'Upgrade-Insecure-Requests': '1',
                                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
                            })

                            resp4 = s.get(
                                'http://delcorealestate.co.delaware.pa.us/pt/datalets/datalet.aspx?mode=tax_delinquent&sIndex=0&idx=1&LMparent=20')
                            if resp4.status_code == 200:
                                resp4soup = soup(resp4.text, 'html.parser')
                                DelinquentTaxes = []
                                tempList = []
                                try:
                                    for tr in resp4soup.find(id='Outstanding Delinquent Taxes').findAll('tr')[1:]:
                                        try:
                                            tempList.append([tr.findAll('td')[0].text, tr.findAll('td')[1].text])
                                        except Exception as e:
                                            pass
                                            # print(f'Tax details not found for parcel ID: {parcel_number}')
                                except Exception as e:
                                    for _ in range(1, 8):
                                        tempList.append(["", ""])
                                # print(tempList)

                                for i in range(0, 7):
                                    if i > 6:
                                        break
                                    try:
                                        if tempList[i][0] == '2021':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2020':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2019':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2018':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2017':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2016':
                                            DelinquentTaxes.append(tempList[i][1])
                                        elif tempList[i][0] == '2015':
                                            DelinquentTaxes.append(tempList[i][1])
                                        else:
                                            DelinquentTaxes.append('')
                                    except:
                                        DelinquentTaxes.append('')

                                final_data = [parcel_number, *DelinquentTaxes, *MortgageCompany, *MortgageService]
                                finalstr = ''
                                for i in final_data:
                                    if ',' in i:
                                        finalstr = f'{finalstr}"{i}",'
                                    else:
                                        finalstr = f'{finalstr}{i},'
                                finalstr = finalstr[:-1]
                                Output_Handle.write(f'{finalstr}\n')
                                Status = True

                                print(f'Extracted parcel number: {parcel_number}.')
            except Exception as e:
                Exception_Handle.write(f"{parcel_number} - {str(e)}\n")
                time.sleep(5)
            finally:
                if Status:
                    Progress_Handle.write(f"{parcel_number}\n")
                Progress_Handle.flush()
                Output_Handle.flush()
                Exception_Handle.flush()


threads = []
for j in range(THREADS):
    threads.append(Check(numbers_q))
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
