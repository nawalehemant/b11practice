num=str(input("number:"))

print(num)

rev_num=num[::-1]
print (rev_num)

if int(num)==int(rev_num):
    print ("number is palindrom")

else:
    print ("number is not palindrom")