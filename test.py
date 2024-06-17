from collections import Counter
l = [1,2,3,4,2,3,4,5,3,2,3,4,6,7,3,4,8,3,4,9]
d = {}
for i in l:
    if i not in d:
        d[i] = 1
    else:
        d[i]+=1


s = 'Mayank loves Data Engineering'
l2 = s.split(" ")
l3 = []
s2 = " "
for i in l2:
    j = i[::-1]
    l3.append(j)

print(s2.join(l3))




