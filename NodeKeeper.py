import uu

uu.encode('scream.mp4', 'video.txt')
uu.decode('video.txt', 'video-copy.mp4')



f = open("scream.mp4", "rb")


FileData=[]
chunk = 1024
while True:
     data = f.read(chunk)
     if not data:
        break
     FileData.append(data)
ff = open("scream2.mp4", "wb")
for chunks in FileData:
    ff.write(chunks)
ff.close()     
