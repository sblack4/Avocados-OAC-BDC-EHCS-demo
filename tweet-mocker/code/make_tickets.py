#!/bin/python 

"""
takes tweet dataset and transforms into tickets dataset
Category, Date, Comment, id

category: ['bug', 'ticket', 'help', 'ID10t', 'feature']
"""

import csv 
from random import choice


header = [ 'id', 'date', 'category', 'comment']
categories = ['bug', 'ticket', 'help', 'ID10t', 'feature']

def randomCategory():
  return choice(categories)

csvreader = csv.DictReader(open('tweets.csv', 'r'))
csvwriter = csv.writer(open('tickets.csv', 'w'), quoting=csv.QUOTE_ALL)

csvwriter.writerow(header)

for row in csvreader:
  date = row['datetime']
  comment = row['text']
  id = row['id']
  cat = randomCategory()
  newRow = [id, date, cat, comment]
  csvwriter.writerow(newRow)

