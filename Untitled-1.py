import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["mydatabase"]
test = mydb["Test"]
Ip = 0
FileName = []
mydict = []
ID = []
for i in range(10):
    mydict.append("")
    FileName.append("")
    ID.append("")
ID[0] = "111"
ID[1] = "111"
ID[2] = "111"
ID[3] = "111"
ID[4] = "213"
ID[5] = "343"
ID[6] = "542"
ID[7] = "123"
ID[8] = "123"
ID[9] = "442"

FileName[0] = "test"
FileName[1] = "test"
FileName[2] = "test"
FileName[3] = "test"
FileName[4] = "dada"
FileName[5] = "hehe"
FileName[6] = "lolo"
FileName[7] = "3a3a"
FileName[8] = "3a3a"
FileName[9] = "kak"


mydict[0] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[1] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[2] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[3] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[4] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[5] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[6] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[7] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[8] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}
mydict[9] = {"ID": ID, "IP": Ip, "FileName": ID+FileName}

for p in mydict:
    for key , value in p.items():
        print(key , value)
        break
    break

#LookUpTable.drop()