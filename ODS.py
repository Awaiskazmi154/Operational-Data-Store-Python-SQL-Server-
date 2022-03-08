import sys

import pyodbc
import pandas as pd
import re
import datetime
import json
from pandas.io.json import json_normalize


class ODS:

    def __init__(self, server, dataWarehouse):
        self.conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={server};"
        f"Database={dataWarehouse};"
        "Trusted_Connection=yes;"
        )
        self.cursor = self.conn.cursor()
        print(" Welcome To ODS ")

    def read(self):
        print("Creating ODS")

        for row in self.cursor.tables("FactStationarySales"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimCustomer"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimProduct"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimEmployee"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimSupplier"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimSales"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimSaleItem"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimStore"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimLocation"):
            print(f' Table = {row}')
        for row in self.cursor.tables("DimTime"):
            print(f' Table = {row}')

        print("ODS Matching to Star/SnowFlake Schema Successfully created!")
        print()

class ExtractDataToODS:

    def __init__(self, server, database,ods):
        self.conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;"
        )
        self.ods_cursor = ods.cursor
        self.database_cursor = self.conn.cursor()
        self.data_DimCustomer = []
        self.data_DimProduct = []
        self.data_DimSupplier = []
        self.data_DimStore = []
        self.data_DimSales = []
        self.data_DimSaleItem = []
        self.data_DimEmployee = []
        self.data_DimLocation = []
        self.data_DimTime = []

    def read(self):
        print("Reading Data from the Database")

        # Customers data
        self.database_cursor.execute("select CustomerID,CustomerEmail,FirstName,SecondName,CustomerType from Customer ")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0,SK)
            self.data_DimCustomer.append(l)
            SK = SK + 1

        # Products data
        self.database_cursor.execute("select ProductID,ProductDescription,ProductPrice,SupplierPrice,CategoryID,SafetyStockLevel,ReorderPoint,SupplierID from Product ")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimProduct.append(l)
            SK = SK + 1

        cat_data = []
        self.database_cursor.execute("select * from Category ")
        for row in self.database_cursor:
            l = list(row)
            cat_data.append(l)

        for rec in self.data_DimProduct:
            for row in cat_data:
                if rec[5] == row[0]:
                    rec.insert(6, row[1])
                    rec.insert(7, row[2])
                    rec.insert(11, None)
                    rec.insert(12, None)

        # Supplier data
        self.database_cursor.execute(
            "select SupplierID,SupplierAddress,SupplierPhone from Supplier ")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimSupplier.append(l)
            SK = SK + 1

        # Store data
        self.database_cursor.execute(
            "select StoreID,StoreAddress,StorePhone from Store ")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimStore.append(l)
            SK = SK + 1

        # Sale data
        self.database_cursor.execute(
            "select SaleID,StaffID,DateOfSale,SaleAmount,SalesTax,SaleTotal,StoreID from StoreSale ")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimSales.append(l)
            SK = SK + 1

        for row in self.data_DimSales:
            row.insert(3, None)
            row.insert(4, None)

        self.database_cursor.execute(
            "select SaleID,CustomerID,DateOfSale,SaleAmount,SalesTax,SaleTotal from InternetSale ")

        index = SK
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimSales.append(l)
            SK = SK + 1

        for row in self.data_DimSales:
            if row[0] >= index:
                row.insert(2, None)
                row.insert(4, None)
                row.insert(9, None)

        # SaleItem data
        self.database_cursor.execute(
            "select SaleID,ProductID,Quantity from SaleItem")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimSaleItem.append(l)
            SK = SK + 1

        for row in self.data_DimSaleItem:
            row.insert(4, None)
            row.insert(5, None)
            row.insert(6, None)

        self.database_cursor.execute(
            "select SaleID,ProductID,Quantity,DateShipped,ShippingType from InternetSaleItem")

        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            l.insert(6, None)
            self.data_DimSaleItem.append(l)
            SK = SK + 1

        # Employee data
        self.database_cursor.execute(
            "select EmployeeID,FirstName,LastName,BirthDate,HireDate,EndDate,EmailAddress,Phone,EmergencyContactPhone,JobTitleID,Status,StoreID from Employee")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            self.data_DimEmployee.append(l)
            SK = SK + 1

        job_data = []
        self.database_cursor.execute(
            "select * from Job")

        for row in self.database_cursor:
            l = list(row)
            job_data.append(l)

        for rec in self.data_DimEmployee:
            for row in job_data:
                if rec[10] == row[0]:
                    rec.insert(11, row[1])
                    rec.insert(12, row[2])
                    rec.insert(13, row[3])
                    rec.insert(14, row[4])
                    rec.insert(15, row[5])
                    rec.insert(16, row[6])
                    rec.insert(17, row[7])
                    rec.insert(18, row[8])

        pay_freq_data = []
        self.database_cursor.execute(
            "select * from PayFrequency")

        for row in self.database_cursor:
            l = list(row)
            pay_freq_data.append(l)

        for rec in self.data_DimEmployee:
            for row in pay_freq_data:
                if rec[15] == row[0]:
                    rec.insert(16, row[1])

        emp_status_data = []
        self.database_cursor.execute(
            "select * from EmployeeStatus")

        for row in self.database_cursor:
            l = list(row)
            emp_status_data.append(l)

        for rec in self.data_DimEmployee:
            for row in emp_status_data:
                if rec[20] == row[0]:
                    rec.insert(21, row[1])

        # Location data
        self.database_cursor.execute("select City,StateProvince,Country,PostalCode from Customer")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            l.insert(5, None)
            l.insert(6, None)
            l.insert(7, None)
            l.insert(8, None)
            l.insert(9, None)
            l.insert(10, None)
            l.insert(11, None)
            l.insert(12, None)
            self.data_DimLocation.append(l)
            SK = SK + 1

        self.database_cursor.execute("select SupplierCity,SupplierStateProvince,SupplierCountry,SupplierPostCode from Supplier")

        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            l.insert(1, None)
            l.insert(2, None)
            l.insert(3, None)
            l.insert(4, None)
            l.insert(9, None)
            l.insert(10, None)
            l.insert(11, None)
            l.insert(12, None)
            self.data_DimLocation.append(l)
            SK = SK + 1

        self.database_cursor.execute(
            "select StoreCity,StoreStateProvince,StoreCountry,StorePostCode from Store")

        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            l.insert(1, None)
            l.insert(2, None)
            l.insert(3, None)
            l.insert(4, None)
            l.insert(5, None)
            l.insert(6, None)
            l.insert(7, None)
            l.insert(8, None)
            self.data_DimLocation.append(l)
            SK = SK + 1

        # Time data
        self.database_cursor.execute("select DateOfSale from StoreSale")

        SK = 1
        for row in self.database_cursor:
            l = list(row)
            l.insert(0, SK)
            l.insert(2, l[1].day)
            l.insert(3, l[1].month)
            l.insert(4, l[1].year)
            self.data_DimTime.append(l)
            SK = SK + 1


    def extract_data_to_ods(self):
        print("Extracting Data From Database to ODS ")
        insert_statement_to_DimCustomer = """ INSERT INTO DimCustomer VALUES (?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimProduct = """ INSERT INTO DimProduct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimSupplier = """ INSERT INTO DimSupplier VALUES (?, ?, ?, ?)"""
        insert_statement_to_DimStore = """ INSERT INTO DimStore VALUES (?, ?, ?, ?)"""
        insert_statement_to_DimSales = """ INSERT INTO DimSales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimSaleItem= """ INSERT INTO DimSaleItem VALUES (?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimEmployee = """ INSERT INTO DimEmployee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimLocation = """ INSERT INTO DimLocation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimTime = """ INSERT INTO DimTime VALUES (?, ?, ?, ?, ?)"""

        try:
            self.ods_cursor.execute("""DELETE FROM DimCustomer""")
            self.ods_cursor.commit()

            for record in self.data_DimCustomer:
                self.ods_cursor.execute(insert_statement_to_DimCustomer, record)

            print("Customers Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimProduct""")
            self.ods_cursor.commit()

            for record in self.data_DimProduct:
                self.ods_cursor.execute(insert_statement_to_DimProduct, record)

            print("Products Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimSupplier""")
            self.ods_cursor.commit()

            for record in self.data_DimSupplier:
                self.ods_cursor.execute(insert_statement_to_DimSupplier, record)

            print("Supplier Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimStore""")
            self.ods_cursor.commit()

            for record in self.data_DimStore:
                self.ods_cursor.execute(insert_statement_to_DimStore, record)

            print("Store Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimSales""")
            self.ods_cursor.commit()

            for record in self.data_DimSales:
                self.ods_cursor.execute(insert_statement_to_DimSales, record)

            print("Sales Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimSaleItem""")
            self.ods_cursor.commit()

            for record in self.data_DimSaleItem:
                self.ods_cursor.execute(insert_statement_to_DimSaleItem, record)

            print("SaleItem Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimEmployee""")
            self.ods_cursor.commit()

            for record in self.data_DimEmployee:
                self.ods_cursor.execute(insert_statement_to_DimEmployee, record)

            print("Employee Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimLocation""")
            self.ods_cursor.commit()

            for record in self.data_DimLocation:
                self.ods_cursor.execute(insert_statement_to_DimLocation, record)

            print("Location Records Extracted Successfully")
            self.ods_cursor.commit()

            self.ods_cursor.execute("""DELETE FROM DimTime""")
            self.ods_cursor.commit()

            for record in self.data_DimTime:
                self.ods_cursor.execute(insert_statement_to_DimTime, record)

            print("Time Records Extracted Successfully")
            self.ods_cursor.commit()

        except:
            print("Records Not Extracted")


class CSVtoODS:

    def __init__(self, server, dataWarehouse, csvdata):
        self.conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={server};"
        f"Database={dataWarehouse};"
        "Trusted_Connection=yes;"
        )
        self.cursor = self.conn.cursor()
        self.csvSalesData = csvdata

    def read(self):
        print("Reading CSV Data From File")

        product_ids = []
        for item in self.csvSalesData.item:
            self.cursor.execute("""SELECT * FROM DimProduct WHERE ProductDescription LIKE ?""", item)
            if self.cursor.rowcount != 0:
                for row in self.cursor:
                    l = list(row)
                    product_ids.append(l[1])
            else:
                product_ids.append(None)

        sale_dates_in_numbers = []
        sale_dates = []
        sale_dates_TimeDim = []

        for date in self.csvSalesData.date:
            numeric_date = [int(s) for s in re.findall(r'-?\d+\.?\d*', date)]
            month = date.split(' ')[3]
            if(month == 'January'):
                month = 1
            elif (month == 'February'):
                month = 2
            elif (month == 'March'):
                month = 3
            elif (month == 'April'):
                month = 4
            elif (month == 'May'):
                month = 5
            elif (month == 'June'):
                month = 6
            elif (month == 'July'):
                month = 7
            elif (month == 'August'):
                month = 8
            elif (month == 'September'):
                month = 9
            elif (month == 'October'):
                month = 10
            elif (month == 'November'):
                month = 11
            elif (month == 'December'):
                month = 12

            numeric_date.insert(1,month)
            if (numeric_date in sale_dates_in_numbers) == False:
                sale_dates_in_numbers.append(numeric_date)
                sale_dates_TimeDim.append(datetime.date(numeric_date[2], numeric_date[1], numeric_date[0]))
            sale_dates.append(datetime.date(numeric_date[2], numeric_date[1], numeric_date[0]))

        sales_quantity = []
        for quantity in self.csvSalesData.quantity:
            sales_quantity.append(quantity)

        self.cursor.execute("select * from DimSaleItem")
        SaleItemSK = 1
        for row in self.cursor:
            SaleItemSK = SaleItemSK + 1

        self.new_saleItem_data_from_csv = []
        temp = []
        index = 0
        for sale_id in self.csvSalesData.sale:
            temp.append(SaleItemSK)
            temp.append(sale_id)
            temp.append(product_ids[index])
            temp.append(sales_quantity[index])
            temp.append(sale_dates[index])
            temp.append(None)
            temp.append(None)
            self.new_saleItem_data_from_csv.append(temp)
            temp = []
            SaleItemSK = SaleItemSK + 1
            index = index + 1
        # print(self.new_saleItem_data_from_csv)

        sales_total = []
        for total in self.csvSalesData.total:
            sales_total.append(total)

        sales_employees = []
        for employee in self.csvSalesData.employee:
            sales_employees.append(employee)

        self.cursor.execute("select * from DimSales")
        SalesSK = 1
        for row in self.cursor:
            SalesSK = SalesSK + 1

        self.new_sales_data_from_csv = []
        temp = []
        index = 0
        for sale_id in self.csvSalesData.sale:
            temp.append(SalesSK)
            temp.append(sale_id)
            temp.append(sales_employees[index])
            temp.append(None)
            temp.append(None)
            temp.append(sale_dates[index])
            temp.append(sales_total[index])
            temp.append(None)
            temp.append(sales_total[index])
            temp.append(None)
            self.new_sales_data_from_csv.append(temp)
            temp = []
            SalesSK = SalesSK + 1
            index = index + 1

        self.cursor.execute("select * from DimTime")
        TimeSK = 1
        for row in self.cursor:
            TimeSK = TimeSK + 1

        self.new_time_data_from_csv = []
        temp = []
        index = 0
        for date in sale_dates_in_numbers:
            temp.append(TimeSK)
            temp.append(sale_dates_TimeDim[index])
            temp.append(date[0])
            temp.append(date[1])
            temp.append(date[2])
            self.new_time_data_from_csv.append(temp)
            temp = []
            TimeSK = TimeSK + 1
            index = index + 1

    def upload_csv_data_to_ods(self):
        print("Uploading CSV data")
        insert_statement_to_DimSales = """ INSERT INTO DimSales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimSaleItem= """ INSERT INTO DimSaleItem VALUES (?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimTime = """ INSERT INTO DimTime VALUES (?, ?, ?, ?, ?)"""

        try:
            for record in self.new_sales_data_from_csv:
                self.cursor.execute(insert_statement_to_DimSales, record)
            self.cursor.commit()

            for record in self.new_saleItem_data_from_csv:
                self.cursor.execute(insert_statement_to_DimSaleItem, record)
            self.cursor.commit()

            for record in self.new_time_data_from_csv:
                self.cursor.execute(insert_statement_to_DimTime, record)
            self.cursor.commit()

            print("CSV Data Uploaded to ODS Successfully")

        except:
            print("Records Not Uploaded")


class JSONtoODS:

    def __init__(self, server, dataWarehouse, jsondata):
        self.conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={server};"
        f"Database={dataWarehouse};"
        "Trusted_Connection=yes;"
        )
        self.cursor = self.conn.cursor()
        self.JSONData = jsondata

    def read(self):
        print("Reading JSON Data from File")

        sales_ids = []
        for sale in self.JSONData['Sale']:
            sales_ids.append(sale['SaleID'])

        sales_customers = []
        for sale in self.JSONData['Sale']:
            sales_customers.append(sale['Customer'])

        sales_delivery = []
        for sale in self.JSONData['Sale']:
            sales_delivery.append(sale['Delivery'])

        sales_dates = []
        sale_dates_in_numbers = []
        sale_dates_TimeDim = []

        for sale in self.JSONData['Sale']:

            numeric_date = [int(s) for s in re.findall(r'-?\d+\.?\d*', sale['DateOfSale'])]
            if (numeric_date in sale_dates_in_numbers) == False:
                sale_dates_in_numbers.append(numeric_date)
                sale_dates_TimeDim.append(datetime.date(numeric_date[2], numeric_date[1], numeric_date[0]))
            sales_dates.append(datetime.date(numeric_date[2], numeric_date[1], numeric_date[0]))

        sales_amount = []
        for sale in self.JSONData['Sale']:
            sales_amount.append(sale['SubTotal'])

        sales_tax = []
        for sale in self.JSONData['Sale']:
            sales_tax.append(sale['SaleTax'])

        sales_total = []
        for sale in self.JSONData['Sale']:
            sales_total.append(sale['SaleTotal'])

        sales_items = []
        for sale in self.JSONData['Sale']:
            for sale_item in (sale['Sales']):
                temp = []
                temp.append(sale['SaleID'])
                temp.append(sale_item['Product'])
                temp.append(sale_item['Quantity'])
                numeric_date = [int(s) for s in re.findall(r'-?\d+\.?\d*', sale['DateOfSale'])]
                temp.append(datetime.date(numeric_date[2], numeric_date[1], numeric_date[0]))
                temp.append(None)
                temp.append(sale_item['Subtotal'])
                sales_items.append(temp)

        self.cursor.execute("select * from DimSales")
        SalesSK = 1
        for row in self.cursor:
            SalesSK = SalesSK + 1

        self.new_sale_data_from_json = []
        temp = []
        index = 0
        for sale_id in sales_ids:
            temp.append(SalesSK)
            temp.append(sale_id)
            temp.append(None)
            temp.append(sales_customers[index])
            temp.append(sales_delivery[index])
            temp.append(sales_dates[index])
            temp.append(sales_amount[index])
            temp.append(sales_tax[index])
            temp.append(sales_total[index])
            temp.append(None)
            self.new_sale_data_from_json.append(temp)
            temp = []
            SalesSK = SalesSK + 1
            index = index + 1

        self.cursor.execute("select * from DimSaleItem")
        SaleItemSK = 1

        for row in self.cursor:
            SaleItemSK = SaleItemSK + 1

        for sale_item in sales_items:
            sale_item.insert(0, SaleItemSK)
            SaleItemSK = SaleItemSK + 1

        self.new_saleItem_data_from_json = sales_items

        self.cursor.execute("select * from DimTime")
        TimeSK = 1
        for row in self.cursor:
            TimeSK = TimeSK + 1

        self.new_time_data_from_json = []
        temp = []
        index = 0
        for date in sale_dates_in_numbers:
            temp.append(TimeSK)
            temp.append(sale_dates_TimeDim[index])
            temp.append(date[0])
            temp.append(date[1])
            temp.append(date[2])
            self.new_time_data_from_json.append(temp)
            temp = []
            TimeSK = TimeSK + 1
            index = index + 1

        #########################################################################
        self.cursor.execute("select * from DimProduct")
        ProductSK = 1

        for row in self.cursor:
            ProductSK = ProductSK + 1

        self.new_product_data_json = []
        temp = []
        for product in self.JSONData['Product']:
            temp.append(product['id'])
            temp.append(product['name'])
            temp.append(product['prices.amountMin'])
            temp.append(product['prices.amountMax'])
            temp.append('Electronics')
            temp.append(None)
            temp.append(None)
            temp.append(None)
            temp.append(None)
            temp.append(None)
            temp.append(product['brand'])
            if product['manufacturer'] == "":
                temp.append(None)
            else:
                temp.append(product['manufacturer'])

            self.new_product_data_json.append(temp)
            temp = []

        for product in self.new_product_data_json:
            product.insert(0, ProductSK)
            ProductSK = ProductSK + 1

    def upload_sales_json_data_to_ods(self):
        print("Uploading Sales JSON data")
        insert_statement_to_DimSales = """ INSERT INTO DimSales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )"""
        insert_statement_to_DimSaleItem= """ INSERT INTO DimSaleItem VALUES (?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimTime = """ INSERT INTO DimTime VALUES (?, ?, ?, ?, ?)"""

        try:
            for record in self.new_sale_data_from_json:
                self.cursor.execute(insert_statement_to_DimSales, record)
            self.cursor.commit()

            for record in self.new_saleItem_data_from_json:
                self.cursor.execute(insert_statement_to_DimSaleItem, record)
            self.cursor.commit()

            for record in self.new_time_data_from_json:
                self.cursor.execute(insert_statement_to_DimTime, record)
            self.cursor.commit()

            print("Sales JSON Data Uploaded to ODS Successfully")

        except:
            print("Records Not Uploaded")

    def upload_product_json_data_to_ods(self):
        print("Uploading Product JSON data")
        insert_statement_to_DimProduct = """ INSERT INTO DimProduct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""

        try:
            for record in self.new_product_data_json:
                self.cursor.execute(insert_statement_to_DimProduct, record)
            self.cursor.commit()

            print("Product JSON Data Uploaded to ODS Successfully")

        except:
            print("Records Not Uploaded")



class ExtractODSToEmptyDB:

    def __init__(self, server, database,ods):
        self.conn = pyodbc.connect(
        "Driver={SQL Server Native Client 11.0};"
        f"Server={server};"
        f"Database={database};"
        "Trusted_Connection=yes;"
        )
        self.ods_cursor = ods.cursor
        self.database_cursor = self.conn.cursor()
        self.data_DimCustomer = []
        self.data_DimProduct = []
        self.data_DimSupplier = []
        self.data_DimStore = []
        self.data_DimSales = []
        self.data_DimSaleItem = []
        self.data_DimEmployee = []
        self.data_DimLocation = []
        self.data_DimTime = []

    def read(self):
        print("Reading Data from the Database")

        # Customers data
        self.ods_cursor.execute("select * from DimCustomer ")

        # Products data
        self.ods_cursor.execute("select * from DimProduct ")

        # Supplier data
        self.ods_cursor.execute("select * from DimSupplier ")

        # Store data
        self.ods_cursor.execute("select * from DimStore ")

        # Sale data
        self.ods_cursor.execute("select * from DimSales ")

        # SaleItem data
        self.ods_cursor.execute("select * from DimSaleItem")

        # Employee data
        self.ods_cursor.execute("select * from DimEmployee")

        # Location data
        self.ods_cursor.execute("select * from DimLocation")

        # Time data
        self.ods_cursor.execute("select * from DimTime")

    def extract_data_to_empty_db(self):
        print("Extracting Data From ODS to DB ")
        insert_statement_to_DimCustomer = """ INSERT INTO DimCustomer VALUES (?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimProduct = """ INSERT INTO DimProduct VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimSupplier = """ INSERT INTO DimSupplier VALUES (?, ?, ?, ?)"""
        insert_statement_to_DimStore = """ INSERT INTO DimStore VALUES (?, ?, ?, ?)"""
        insert_statement_to_DimSales = """ INSERT INTO DimSales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimSaleItem= """ INSERT INTO DimSaleItem VALUES (?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimEmployee = """ INSERT INTO DimEmployee VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimLocation = """ INSERT INTO DimLocation VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        insert_statement_to_DimTime = """ INSERT INTO DimTime VALUES (?, ?, ?, ?, ?)"""

        try:
            self.database_cursor.execute("""DELETE FROM DimCustomer""")
            self.database_cursor.commit()

            for record in self.data_DimCustomer:
                self.database_cursor.execute(insert_statement_to_DimCustomer, record)

            print("Customers Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimProduct""")
            self.database_cursor.commit()

            for record in self.data_DimProduct:
                self.database_cursor.execute(insert_statement_to_DimProduct, record)

            print("Products Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimSupplier""")
            self.database_cursor.commit()

            for record in self.data_DimSupplier:
                self.database_cursorr.execute(insert_statement_to_DimSupplier, record)

            print("Supplier Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimStore""")
            self.database_cursor.commit()

            for record in self.data_DimStore:
                self.database_cursor.execute(insert_statement_to_DimStore, record)

            print("Store Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimSales""")
            self.database_cursor.commit()

            for record in self.data_DimSales:
                self.database_cursor.execute(insert_statement_to_DimSales, record)

            print("Sales Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimSaleItem""")
            self.database_cursor.commit()

            for record in self.data_DimSaleItem:
                self.database_cursor.execute(insert_statement_to_DimSaleItem, record)

            print("SaleItem Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimEmployee""")
            self.database_cursor.commit()

            for record in self.data_DimEmployee:
                self.database_cursor.execute(insert_statement_to_DimEmployee, record)

            print("Employee Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimLocation""")
            self.database_cursor.commit()

            for record in self.data_DimLocation:
                self.database_cursor.execute(insert_statement_to_DimLocation, record)

            print("Location Records Extracted Successfully")
            self.database_cursor.commit()

            self.database_cursor.execute("""DELETE FROM DimTime""")
            self.database_cursor.commit()

            for record in self.data_DimTime:
                self.database_cursor.execute(insert_statement_to_DimTime, record)

            print("Time Records Extracted Successfully")
            self.database_cursor.commit()

        except:
            print("Records Not Extracted")

def main():

    print()
    print()
    while True:
        print(" ---------------   WELCOME TO ODS   -----------------------")
        print(" 1 - Create Operational Data Store Matching to Datawarehouse Schema")
        print(" 2 - Extract Data from the Database to ODS ")
        print(" 3 - Upload Sales CSV data to ODS ")
        print(" 4 - Upload Sales JSON data to ODS ")
        print(" 5 - Upload Product JSON data to ODS ")
        print(" 6 - Export ODS data to empty Database ")
        print(" 7 - Exit")
        print()
        try:
            choice = int(input(" Enter your choice: "))
            if choice == 1:
                try:
                    ods = ODS("DESKTOP-K8V27P9", "StationaryDatawarehouse")
                    ods.read()
                except:
                    print('connection error')
                print("######################################")

            elif choice == 2:
                try:
                    ods = ODS("DESKTOP-K8V27P9", "StationaryDatawarehouse")
                    extract_to_ods = ExtractDataToODS("DESKTOP-K8V27P9", "StationaryDatabase",ods)
                    extract_to_ods.read()
                    extract_to_ods.extract_data_to_ods()
                    extract_to_ods.database_cursor.close()
                    extract_to_ods.ods_cursor.close()
                except:
                    print('connection error')
                print("######################################")

            elif choice == 3:
                try:
                    datacsv = pd.read_csv("AssignmentDataFiles/SalesCSV.csv")
                except:
                    print('File Read Error')
                try:
                    uploadCsv = CSVtoODS("DESKTOP-K8V27P9", "StationaryDatawarehouse", datacsv)
                    uploadCsv.read()
                    uploadCsv.upload_csv_data_to_ods()
                except:
                    print('connection error')
                print("######################################")

            elif choice == 4:
                try:
                    with open('AssignmentDataFiles/SalesJSON.json') as json_data:
                        jsondata = json.load(json_data)
                except:
                    print('File Read Error')
                try:
                    uploadJson = JSONtoODS("DESKTOP-K8V27P9", "StationaryDatawarehouse", jsondata)
                    uploadJson.read()
                    uploadJson.upload_sales_json_data_to_ods()
                except:
                    print('connection error')
                print("######################################")

            elif choice == 5:
                with open('AssignmentDataFiles/SalesJSON.json') as json_data:
                    jsondata = json.load(json_data)
                try:
                    uploadJson = JSONtoODS("DESKTOP-K8V27P9", "StationaryDatawarehouse", jsondata)
                    uploadJson.read()
                    uploadJson.upload_product_json_data_to_ods()
                except:
                    print('connection error')
                print("######################################")

            if choice == 6:
                try:
                    ods = ODS("DESKTOP-K8V27P9", "StationaryDatawarehouse")
                    extract_ods_to_database = ExtractODSToEmptyDB("DESKTOP-K8V27P9", "EmptyDatabase", ods)
                    extract_ods_to_database.extract_data_to_empty_db()
                except:
                    print('connection error')
                print("######################################")

            elif choice == 7:
                print(' -----------------        THANKS FOR VISITING ODS     ------------------')
                print("######################################")
                sys.exit()

            elif choice < 1 or choice > 7:
                print("  XXXXXXXXXXX      WRONG CHOICE     XXXXXXXXXXX")
        except:
            if choice == 7:
                sys.exit()
            print("  XXXXXXXXXXX      WRONG CHOICE     XXXXXXXXXXX")

if __name__ == "__main__":
    main()
