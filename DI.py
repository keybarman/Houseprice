import pandas as pd

def salary_cities():

    cities = {
        'A': '臺北市', 'B': '桃園市', 'C': '臺中市', 'D': '臺南市', 'E': '高雄市',
        'F': '新竹縣', 'G': '新竹市'
        }
    

    house_prices ={
        'A': '', 'B': '桃園市', 'C': '臺中市', 'D': '臺南市', 'E': '高雄市',
        'F': '新竹縣', 'G': '新竹市'
        }

    while True:
        try:
            salary=input(int("請輸入你的薪水"))
            if salary >=0:
                break
            print("薪水不能為負!")

        except ValueError:
            print("請輸入有效數字!")

    print("你的薪水是",{salary:}"元")
    print("請選擇你的居住城市:")
    

    for i in range(0,len(items),3):

