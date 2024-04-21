def excel_range(column_num, row_num):
    column_num += 1
    power = 26
    if column_num > 26:
        dro = column_num // power
        print(dro)
        bro = column_num % power
        print(bro)
        print(f"{chr(dro+64)}{chr(bro+65)}{row_num}:{chr(dro+64)}{chr(bro+65)}")
    else:
        print(f"{chr(column_num+64)}{row_num}:{chr(column_num+64)}")

# Пример использования функции
column_num = 51
row_num = 1
excel_range_str = excel_range(column_num, row_num)
