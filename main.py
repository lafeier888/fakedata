import random
import sys
import threading
import time

from faker import Faker

locale_list = ['en-US', 'zh_CN']
fake = Faker(locale_list, use_weighting=False) # false速度会快点

prefix = ''  # 命令行参数1
totalRows = 10  # 命令行参数2
fieldNum = 6  # 命令行参数3
totalThreads = 1  # 不要改，测试效果多线程不如单线程
writeBuffer = 1024

if len(sys.argv) >= 2:
    prefix = 'p' + str(sys.argv[1])

if len(sys.argv) >= 3:
    totalRows = int(sys.argv[2])

if len(sys.argv) >= 4:
    fieldNum = int(sys.argv[3])

# 拼建表语句
fields = []
for i in range(1, fieldNum + 1):
    fields.append("f{}".format(i))

schema = ",".join(["{} STRING".format(f) for f in fields])

createTableSql = "CREATE TABLE test("
createTableSql += schema
createTableSql += ")"
createTableSql += """
ROW FORMAT
  DELIMITED FIELDS TERMINATED BY ','
;
"""
# print("建表语句")
# print(createTableSql)

rules = [
    lambda: fake.bothify("??-??????"),
    lambda: fake.hexify("^^:^^:^^:^^:^^:^^"),
    lambda: fake.locale(),
    lambda: fake.numerify(text='Intel Core i%-%%##K vs AMD Ryzen % %%##X'),
    lambda: fake.random_digit(),
    lambda: fake.random_digit_not_null(),
    lambda: fake.random_int(min=0, max=15),
    lambda: fake.random_number(fix_len=True),
    lambda: fake.random_number(digits=3),
    lambda: fake.random_number(digits=3, fix_len=True),
    lambda: fake.address(),
    lambda: fake.building_number(),
    lambda: fake.country(),
    lambda: fake.current_country(),
    lambda: fake.current_country_code(),
    lambda: fake.country_code(),
    lambda: fake.city(),
    lambda: fake.postcode(),
    lambda: fake.city_suffix(),
    lambda: fake.street_address(),
    lambda: fake.street_name(),
    lambda: fake.street_suffix(),
    lambda: fake.aba(),
    lambda: fake.bank_country(),
    lambda: fake.bban(),
    lambda: fake.iban(),
    lambda: fake.swift(),
    lambda: fake.color_name(),
    lambda: fake.hex_color(),
    lambda: fake.company(),
    lambda: fake.credit_card_number(),
    lambda: fake.currency_name(),
    lambda: fake.date(),
    lambda: fake.day_of_week(),
    lambda: fake.iso8601(),
    lambda: fake.time(),
    lambda: fake.timezone(),
    lambda: fake.year(),
    lambda: fake.file_name(category='audio'),
    lambda: fake.file_path(depth=2),
    lambda: fake.mime_type(category='application'),
    lambda: fake.longitude(),
    lambda: fake.ascii_company_email(),
    lambda: fake.company_email(),
    lambda: fake.isbn10(),
    lambda: fake.job(),
    lambda: fake.first_name(),
    lambda: fake.first_name_female(),
    lambda: fake.first_name_male(),
    lambda: fake.first_name_nonbinary(),
    lambda: fake.language_name(),
    lambda: fake.last_name(),
    lambda: fake.last_name_female(),
    lambda: fake.name(),
    lambda: fake.country_calling_code(),
    lambda: fake.phone_number(),
    lambda: fake.ssn(),
    lambda: fake.android_platform_token(),
    lambda: fake.chrome(),
    lambda: fake.firefox(),
    lambda: fake.internet_explorer(),
    lambda: fake.ios_platform_token(),
    lambda: fake.linux_platform_token(),
    lambda: fake.linux_processor(),
    lambda: fake.mac_platform_token(),
    lambda: fake.opera(),
    lambda: fake.windows_platform_token(),
]
usedRules = []
for field in fields:
    ruleIndex = random.randrange(0, len(rules))
    usedRules.append(ruleIndex)


def writeData(partitionIndex, filename, rows):
    # print((partitionIndex, filename, rows,"\n"))
    # return
    f = open(prefix + '-' + filename, 'w', writeBuffer)
    start = int(time.time())
    flag = True
    for i in range(0, rows):
        columns = []
        columns.append(str(partitionIndex) + '-' + str(i))  # inner id
        for index in usedRules:
            rulelambda = rules[index]
            v = rulelambda()
            columns.append(str(v).replace("\n", '').replace("\\","").replace(',',''))
        row = ','.join(columns)
        f.write(row)
        f.write("\n")
        
        # if flag and (int(time.time()) - start) % 5 ==0:
        #     print((partitionIndex, i))
        #     flag = False
    f.flush()
    f.close()


batchSize = int(totalRows / totalThreads)
threadList = []
for i in range(0, totalThreads):
    partitionIndex = i + 1
    t = threading.Thread(target=writeData, args=(partitionIndex, 'data' + str(partitionIndex) + '.csv', batchSize))
    t.start()
    threadList.append(t)

if totalRows % totalThreads > 0:
    remain = totalRows % totalThreads
    partitionIndex = totalThreads + 1

    t = threading.Thread(target=writeData, args=(partitionIndex, 'data' + str(partitionIndex) + '.csv', remain))
    t.start()
    threadList.append(t)

for t in threadList:
    t.join()

# print("load data local inpath '/Users/lafeier/dev/python/myfakedata/data.csv' into table test")
