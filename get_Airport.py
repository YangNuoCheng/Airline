#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright By Dayyang in 2020

import re
import time
import json
import random
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
import phantomjs
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

class getAirports(object):
	def __init__(self):
		self.client = MongoClient('localhost', port=27017)
		self.db = self.client.Airplane
		self.collection = self.db.Basic
		# self.collection.drop()
		self.collection2 = self.db.Airports
		# self.collection2.drop()
		self.collection3 = self.db.AirLines
		self.collection3.drop()
		self.url = "https://www.flightradar24.com/data/airports"
		self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36'}
		self.chrome_options = Options()
		self.chrome_options.add_argument('--headless')
		self.chrome_options.add_argument('--disable-gpu')
		self.driver = webdriver.Chrome(chrome_options=self.chrome_options)

	def getFile(self):
		path = '/Users/yangnuocheng/git/AirLineAcrossChina/Search for Airports - Flightradar24.html'
		htmlfile = open(path, 'r', encoding='utf-8')
		htmlhandle = htmlfile.read()
		return htmlhandle
	def getHtml(self, url):     # 获取网页数据
		self.driver.get(url)
		# reps = requests.get(url=url, headers=self.headers)
		soup = BeautifulSoup(self.driver.page_source, 'lxml')
		return soup
	def downloadCountry(self):
		# soup = BeautifulSoup(self.getFile(), 'lxml')
		soup = self.getHtml(self.url)
		all_tr = soup.find(id="tbl-datatable").find_all('tr')
		for tr in all_tr:
			try:
				airports_links = tr.find_all("a",{"href":re.compile("https://www.flightradar24.com/data/airports\.*?")})
				# print(airports_links[0]["data-country"])
				DB__countary = airports_links[0]["data-country"]
				DB__url = airports_links[0]["href"]
				# print(airports_links[0]["href"])
				img_links = tr.find_all("img",{"data-bn-lazy-src":re.compile(".*?\.gif")})
				# print(img_links[0]["data-bn-lazy-src"])
				DB__img = img_links[0]["data-bn-lazy-src"]
				number_links=tr.find_all("span",{"class":"gray pull-right"})
				# print(number_links[0].text.split(' ')[1])
				DB__number = int(number_links[0].text.split(' ')[1])

				flight = {
					"DB__countary":DB__countary,
					"DB__url":DB__url,
					"DB__img":DB__img,
					"DB__number":DB__number
					}
				self.collection.insert_one(flight)
				# print("Complete")
			except LookupError:      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
				continue

	def downloadAirports(self,dic):
		soup = self.getHtml(dic['DB__url'])
		# print(type(soup)) # <class 'bs4.BeautifulSoup'>
		# print(soup)
		all_tr = soup.find(id="tbl-datatable").find_all('tr')
		for tr in all_tr:
			try:
				airports_links = tr.find_all("a",{"href":re.compile("https://www.flightradar24.com/data/airports\.*?")})
				Country = dic['DB__countary']
				Name = airports_links[0]["title"]
				Url = airports_links[0]["href"]
				Iata = airports_links[0]["data-iata"]
				Lat = airports_links[0]["data-lat"]
				Lon = airports_links[0]["data-lon"]
				airports = {
					"Iata":Iata,
					"Country":Country,
					"Url":Url,
					"Lat":Lat,
					"Lon":Lon,
				}
				self.collection2.insert_one(airports)
				print(str(airports_links[0]["data-iata"])+"插入完成")
			except LookupError:      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
				continue
	def downloadAirlines(self,dic):
		soup = self.getHtml(dic['Url'])
		all_tr = soup.find("tbody").find_all("tr",{"class":"hidden-md hidden-lg ng-scope"})
		for tr in all_tr:
			try:
				div_list = tr.find_all("div",{"class":"row"})
				div_listI = div_list[1].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs"})
				Departures_time = div_listI[0].span.text
				Flight_number = div_listI[1].a.text
				div_listII = div_list[1].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Dest_city = div_listII[0].span.text
				Dest_Iata = div_listII[0].a.text.split('(')[1].split(')')[0]
				div_listIII = div_list[2].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs ng-binding"})
				Aircraft_type = div_listIII[0].text
				div_listIV = div_list[2].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Airline = div_listIV[0].a.text
				# print(Airline)
				plane = {
					"Departures_time": Departures_time,
					"Flight_number" : Flight_number,
					"Dest_city" : Dest_city,
					"Dest_Iata" : Dest_Iata,
					"Aircraft_type" : Aircraft_type,
					"Airline" : Airline,
				}
				self.collection3.insert_one(plane)
				print(str(plane["Flight_number"])+"插入完成")
			except LookupError:      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
				continue

	def Country(self):
		self.downloadCountry()


	def Airports(self):
		countarylist = self.collection.find()
		for countary in countarylist:
			self.downloadAirports(countary)

	def AirLines(self):
		airportslist = self.collection2.find().batch_size(10)
		# Mongodb默认每次下载100条数据，但是如果处理100条数据的时间超过十分钟则会掉线
		# 设置batch_size(10)可以经常性的访问数据库，防止掉线
		for airports in airportslist:
			self.downloadAirlines(airports)

def main():
	flight = getAirports()
	# flight.Country()
	# flight.Airports()
	flight.AirLines()
if __name__ == '__main__':
	main()