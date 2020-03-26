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
from collections import deque
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
		# self.collection3.drop()
		self.collection4 = self.db.computedAirline
		# self.collection4.drop()
		self.url = "https://www.flightradar24.com/data/airports"
		self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.100 Safari/537.36'}
		self.chrome_options = Options()
		self.chrome_options.add_argument('--headless')
		self.chrome_options.add_argument('--disable-gpu')
		self.driver = webdriver.Chrome(chrome_options=self.chrome_options)
		self.alreadyIN = []
		self.queue = deque()
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
	def downloadAirlines_Arrivals(self,dic):
		soup = self.getHtml(str(dic['Url'])+"/arrivals")
		all_tr = soup.find("tbody").find_all("tr",{"class":"hidden-md hidden-lg ng-scope"})
		for tr in all_tr:
			try:
				div_list = tr.find_all("div",{"class":"row"})
				div_listI = div_list[1].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs"})
				Departures_time = div_listI[0].span.text
				Flight_number = div_listI[1].a.text
				div_listII = div_list[1].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Local_city = div_listII[0].span.text
				Local_Iata = div_listII[0].a.text.split('(')[1].split(')')[0]
				div_listIII = div_list[2].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs ng-binding"})
				Aircraft_type = div_listIII[0].text
				div_listIV = div_list[2].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Airline = div_listIV[0].a.text
				# print(Airline)
				Attribute = "Arrival"
				plane = {
					"Departures_time": Departures_time,
					"Flight_number" : Flight_number,
					"Dest_city" : Local_city,
					"Dest_Iata" : Local_Iata,
					"Aircraft_type" : Aircraft_type,
					"Airline" : Airline,
					"Attribute" : Attribute,
				}
				self.collection3.insert_one(plane)
				print(str(plane["Flight_number"])+"插入完成(A)")
			except LookupError:      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
				continue
	def downloadAirlines_Departures(self,dic):
		soup = self.getHtml(str(dic['Url'])+"/departures")
		all_tr = soup.find("tbody").find_all("tr",{"class":"hidden-md hidden-lg ng-scope"})
		for tr in all_tr:
			try:
				div_list = tr.find_all("div",{"class":"row"})
				div_listI = div_list[1].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs"})
				Departures_time = div_listI[0].span.text
				Flight_number = div_listI[1].a.text
				div_listII = div_list[1].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Local_city = div_listII[0].span.text
				Local_Iata = div_listII[0].a.text.split('(')[1].split(')')[0]
				div_listIII = div_list[2].find_all("div",{"class":"col-xs-3 col-sm-3 p-xxs ng-binding"})
				Aircraft_type = div_listIII[0].text
				div_listIV = div_list[2].find_all("div",{"class":"col-xs-6 col-sm-6 p-xxs"})
				Airline = div_listIV[0].a.text
				# print(Airline)
				Attribute = "Departures"
				plane = {
					"Departures_time": Departures_time,
					"Flight_number" : Flight_number,
					"Dest_city" : Local_city,
					"Dest_Iata" : Local_Iata,
					"Aircraft_type" : Aircraft_type,
					"Airline" : Airline,
					"Attribute" : Attribute,
				}
				self.collection3.insert_one(plane)
				print(str(plane["Flight_number"])+"插入完成(D)")
			except LookupError:      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
				continue

	def Lookupsame(self,plane):
		for element in self.queue:
			if "_id" in element:
				del element["_id"]
			# print(element)
			# print(plane)
			if(element == plane):
				return True
		if len(self.queue)>11:
			self.queue.popleft()
		self.queue.append(plane)
		return False

	def getEveryAirline(self,dic):
		soup = self.getHtml("https://www.flightradar24.com/data/flights/"+str(dic['Flight_number']))
		print("Flght:"+str(dic['Flight_number']))
		try:
			all_tr = soup.find("tbody").find_all("tr",{"class":"data-row"})
		except AttributeError:
			pass
		else:
			print("Numbers:"+str(len(all_tr)))
			for tr in all_tr:
				try:
					td_list = tr.find_all("td")
					td_I = td_list[3]
					td_II = td_list[4]
					td_III = td_list[5]
					td_IV = td_list[7]
					td_V = td_list[9]
					plane = {
							"Flight_number" : dic['Flight_number'],
							"Arrive_time" : td_V.text,
							"Departures_time" : td_IV.text,
							"From_City": td_I.text.split('  ')[1].split(' ')[0],
							"To_City": td_II.text.split('  ')[1].split(' ')[0],
							"From_Iata" : td_I.a.text.split('(')[1].split(')')[0],
							"To_Iata" : td_II.a.text.split('(')[1].split(')')[0],
							"Aircraft_type" : dic['Aircraft_type'],
							"Airline" : dic['Airline'],
						}
					if self.Lookupsame(plane) == False:
						self.collection4.insert_one(plane)
						print(str(plane['From_City'])+"->"+str(plane["Flight_number"])+"->"+str(plane['To_City'])+"插入完成(Combine)"+"维护队列长度"+str(len(self.queue)))
					else:
						print(str(plane['From_City'])+"->"+str(plane["Flight_number"])+"->"+str(plane['To_City'])+"重复插入(Combine)"+"维护队列长度"+str(len(self.queue)))
				except(LookupError,AttributeError):      # 捕获映射或序列上使用的键或索引无效时引发的异常的基类
					continue

	def Country(self):
		self.downloadCountry()
	def Airports(self):
		countarylist = self.collection.find()
		for countary in countarylist:
			self.downloadAirports(countary)
	def AirLines_Arrivals(self):
		airportslist = self.collection2.find().batch_size(10)
		# Mongodb默认每次下载100条数据，但是如果处理100条数据的时间超过十分钟则会掉线
		# 设置batch_size(10)可以经常性的访问数据库，防止掉线
		for airports in airportslist:
			self.downloadAirlines_Arrivals(airports)
	def AirLines_Departures(self):
		airportslist = self.collection2.find().batch_size(10)
		# Mongodb默认每次下载100条数据，但是如果处理100条数据的时间超过十分钟则会掉线
		# 设置batch_size(10)可以经常性的访问数据库，防止掉线
		for airports in airportslist:
			self.downloadAirlines_Departures(airports)
	def computeAirline(self):
		# 把离开地与目的地相互关联，方便在 graph 中添加关系节点
		airlinelist = self.collection3.find().batch_size(10)
		for airline in airlinelist:
			try:
				AirLines_arrival = self.collection3.find_one({ "Flight_number": airline['Flight_number'],"Attribute":'Arrival'})   #到达的航班
				AirLines_departure = self.collection3.find_one({ "Flight_number": airline['Flight_number'],"Attribute":'Departures'}) #离开的航班
				if AirLines_arrival['Flight_number'] not in self.alreadyIN :
					plane = {
						"Flight_number" : AirLines_arrival['Flight_number'],
						"Arrive_time" : AirLines_arrival['Departures_time'],
						"Departures_time" : AirLines_departure['Departures_time'],
						"From_City": AirLines_arrival['Dest_city'],
						"To_City": AirLines_departure['Dest_city'],
						"From_Iata" : AirLines_arrival['Dest_Iata'],
						"To_Iata" : AirLines_departure['Dest_Iata'],
						"Aircraft_type" : AirLines_arrival['Aircraft_type'],
						"Airline" : AirLines_arrival['Airline'],
					}
					self.collection4.insert_one(plane)
					self.alreadyIN.append(plane["Flight_number"])
					print(str(plane["Flight_number"])+"插入成功(Combine)")
				else:
					raise IndexError
			except IndexError:
				continue
	
	def EveryAirline(self):
		AirLines = self.collection3.find().batch_size(5)
		for airlines in AirLines:
			self.getEveryAirline(airlines)
	def washData(self):
		DataList = self.collection4.find().batch_size(50)
		for data in DataList:
			values = list(data.values())
			for x in values:
				if x == '  —  ':
					self.collection4.delete_one({"_id":values[0]})
					break

def main():
	flight = getAirports()
	# flight.Country()
	# flight.Airports()
	# flight.AirLines_Arrivals()
	# flight.AirLines_Departures()
	# flight.EveryAirline()
	flight.washData()
if __name__ == '__main__':
	main()