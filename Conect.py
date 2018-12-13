LOGING = True
DEBUG = False

#для запросів
import requests

#зчитування json
import json
from pprint import pprint

#для паузи
import os
import sys

#для компіляції
import decimal

#підключення до БД
import pyodbc

#робота з датою
from datetime import datetime

#functions--------------------------------------------------------------------------------------------------------------
def addCustomer(customerName='', customerEmail='', customerAddress='', customerPhone='', customerDiscount=0):

     insertQuery = "INSERT INTO BASE_Customer (Name, Email, Address1, Phone, Discount, Version, VendorPermitNumber, Remarks, DefaultCarrier, DefaultPaymentMethod, ContactName, Fax, Address2, City, State,Country ,PostalCode ,AddressRemarks ,UsingBillingAddress ,BillingAddress1 ,BillingAddress2 ,BillingCity ,BillingState ,BillingCountry ,BillingPostalCode ,BillingAddressRemarks ,UsingShippingAddress ,ShippingAddress1 ,ShippingAddress2 ,ShippingCity ,ShippingState ,ShippingCountry ,ShippingPostalCode ,ShippingAddressRemarks ,Custom1 ,Custom2 ,Custom3 ,Custom4 ,Custom5 ,CCardNum ,CCardSecurityNum ,CCardExpDate ,LastModUserId, LastModDttm, Timestamp ,IsActive ,Custom6 ,Custom7 ,Custom8 ,Custom9 ,Custom10 ,Website ,DefaultSalesRep) "\
                    +"VALUES (?, ?, ?, ?, ?, 1, '', '', '', '', '', '', '', '', '', '', '', '', 0, '', '', '', '', '', '', '', 0, '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', 100, GETDATE(), DEFAULT, 1, '', '', '', '', '', '', '')"

     if DEBUG:
        print(insertQuery)
        print(customerName, customerEmail, customerAddress, customerPhone, customerDiscount)
     cursor.execute(insertQuery, (customerName, customerEmail, customerAddress, customerPhone, customerDiscount))
     cnxn.commit()

     if DEBUG:
         print('створення вдалось')

     #возврат ид
     cursor.execute("SELECT @@IDENTITY")
     row = cursor.fetchone()
     return row[0]

def updateCustomer(id, customerName='', customerEmail='', customerAddress='', customerPhone='', customerDiscount=0):

    updateQuery = "UPDATE BASE_Customer SET Name = ?, Email = ?, Address1 = ?, Phone = ?, Discount = ? WHERE CustomerId = ?;"

    if DEBUG:
        print(updateQuery)

    cursor.execute(updateQuery, (customerName, customerEmail, customerAddress, customerPhone, customerDiscount, id))
    cnxn.commit()

    if DEBUG:
        print('оновлення вдалось')

def uniqueName(jsonCustomer, customersDB):

    uniqueNameFlag = True

    for customerDB in customersDB:
        # print(jsonCustomer['name'].lower(), '==', customerDB[2].lower())
        # print(bytes(jsonCustomer['name'].lower(), 'UTF-8'), '==', bytes(customerDB[2].lower(), 'UTF-8'))
        if jsonCustomer['name'].lower() == customerDB[2].lower().strip():
            if DEBUG:
                print('совпали імена')
            uniqueNameFlag = False
            break
    i = 0
    while not uniqueNameFlag:
        uniqueNameFlag = True
        i += 1
        for customerDB in customersDB:  # провірка чи імя+1 нема в базі
            if (jsonCustomer['name'] + str(i)).lower() == customerDB[2].lower().strip():  # провірка на співпадання имя
                uniqueNameFlag = False
        if not uniqueNameFlag:
            continue
        jsonCustomer['name'] = jsonCustomer['name'] + str(i)
        if DEBUG or LOGING:
            print(jsonCustomer['name'])
    return jsonCustomer

def delCutomer(chislo = 0):

    if chislo != 0:
        where = ' WHERE CustomerId = ?'
    else:
        where = ' WHERE CustomerId > ?'
        chislo = 103

    #удаление ордерів
    query = "SELECT SalesOrderId FROM SO_SalesOrder" + where
    cursor.execute(query, chislo)
    orders = cursor.fetchall()

    for order in orders:
        delOrsder(order[0])

    query = "DELETE FROM BASE_Customer" + where
    cursor.execute(query, chislo)
    cnxn.commit()

def phoneCheck(jsonPhone, DBphone):
    if jsonPhone == '':
        return False
    # провірка номера телефона перед створенням
    phonesDB = DBphone.split(',')  # розділення номерів по ,
    for phoneDB in phonesDB:
        for phoneDBSpace in phoneDB.split():  # розділення номерів по пробелу
            if phoneDBSpace:
                phoneDBSpace = phoneDBSpace.replace('-', '')
                phoneDBSpace = phoneDBSpace.replace('(', '')
                phoneDBSpace = phoneDBSpace.replace(')', '')
                jsonPhone = jsonPhone.replace('-', '')
                jsonPhone = jsonPhone.replace('(', '')
                jsonPhone = jsonPhone.replace(')', '')
                # print(jsonPhone[-10:], '==', phoneDBSpace[-10:])
                if jsonPhone[-10:] == phoneDBSpace[-10:]:
                    return True

    return False

def CustomerSyn(jsonCustomer, customersRelationship, update = True):

    # зчитування покупців з БД
    cursor.execute("SELECT CustomerId, Email, Name, Address1, Phone, Discount FROM BASE_Customer")
    customersDB = cursor.fetchall()

    # флаги, змінні
    createFlag = True
    newCusId = 0

    # перевірка на співпадання id і ящо ні то перевірка телефона
    for customerDB in customersDB:
        if int(jsonCustomer['inFlow_user_id']) == int(customerDB[0]) and update:  # провірка на співпадання id
            createFlag = False
            # провірка не співпадання хоть одного із параметрів для оновлення
            if jsonCustomer['email'] != customerDB[1].strip() or jsonCustomer['address'].lower().strip() != customerDB[3].lower().strip() \
                    or jsonCustomer['phone'] != customerDB[4] or float(jsonCustomer['discount']) != float(customerDB[5]):
                if LOGING:
                    print('Обновляю пользоватіля:'+'\r\n'
                          +' ід - ' + str(customerDB[0])+'\r\n'
                          + ' Імя - ' + jsonCustomer['name']+'\r\n'
                          + ' адреса - ' + jsonCustomer['address']+'\r\n'
                          + ' телефон - ' + jsonCustomer['phone']+'\r\n'
                          + ' скидка - ' + str(jsonCustomer['discount'])+'\r\n'
                          )
                if DEBUG:
                    print('Обновляю пользоватіля')

                jsonCustomer = uniqueName(jsonCustomer, customersDB)

                # оновлення користувача
                updateCustomer(customerDB[0], jsonCustomer['name'], jsonCustomer['email'], jsonCustomer['address'],
                               jsonCustomer['phone'], jsonCustomer['discount'])

            break
        else:
            # провірка номера телефона перед створенням
            if phoneCheck(jsonCustomer['phone'], customerDB[4]):
                if LOGING or DEBUG:
                    print('Покупатель с таким номером телефона уже существует. Ид - ' + str(customerDB[0]))
                customersRelationship.append({jsonCustomer['id']: str(customerDB[0])})
                return customersRelationship
            if jsonCustomer['email'] == customerDB[1].strip():
                if LOGING or DEBUG:
                    print('Покупатель с таким email уже существует. Ид - ' + str(customerDB[0]))
                customersRelationship.append({jsonCustomer['id']: str(customerDB[0])})
                return customersRelationship

    # защіта от дублікатів імен
    if createFlag:
        jsonCustomer = uniqueName(jsonCustomer, customersDB)

        # Створення нового покупця
        print(jsonCustomer)
        if LOGING:
            print('Создаю пользоватіля:'+'\r\n'
                  + ' Email - ' + jsonCustomer['email']+'\r\n'
                  + ' Імя - ' + jsonCustomer['name']+'\r\n'
                  + ' адреса - ' + jsonCustomer['address']+'\r\n'
                  + ' телефон - ' + jsonCustomer['phone']+'\r\n'
                  + ' скидка - ' + str(jsonCustomer['discount'])+'\r\n'
                  )
        if DEBUG:
            print('Создаю пользоватіля')
        newCusId = addCustomer(jsonCustomer['name'], jsonCustomer['email'], jsonCustomer['address'],
                               jsonCustomer['phone'], jsonCustomer['discount'])

        if DEBUG or LOGING:
            print('---------------------------------------------')

    if newCusId:
        customersRelationship.append({jsonCustomer['id']: str(newCusId)})
    # запис сворено ид для зворотнього звязку


    return customersRelationship


def currencyDB():
    # зчитування валют і їх ід
    cursor.execute("""
    SELECT Name, PricingSchemeId
    FROM BASE_PricingScheme
    """)  #
    currencysDB = cursor.fetchall()

    # переведення виборки БД в удобний виглад
    currency={}
    for currencyDB in currencysDB:
        if currencyDB[0] == currencyNames['UAH']:
            currency['UAH'] = currencyDB[1]
        if currencyDB[0] == currencyNames['EUR']:
            currency['EUR'] = currencyDB[1]
        if currencyDB[0] == currencyNames['USD']:
            currency['USD'] = currencyDB[1]
    return currency

def orderCurrencyDB():
    # зчитування валют і їх ід
    cursor.execute("""
    SELECT Code, CurrencyId
    FROM GLOBAL_Currency
    """)  #
    currencysDB = cursor.fetchall()

    # переведення виборки БД в удобний виглад
    currency={}
    for currencyDB in currencysDB:
        if currencyDB[0] == 'UAH':
            currency['UAH'] = currencyDB[1]
        if currencyDB[0] == 'EUR':
            currency['EUR'] = currencyDB[1]
        if currencyDB[0] == 'USD':
            currency['USD'] = currencyDB[1]
    return currency

def delProduct(chislo = 0):


    if chislo != 0:
        where = ' WHERE ProdId = ?'
    else:
        where = ' WHERE ProdId > ?'
        chislo = 107

    query = "DELETE FROM BASE_Product_Version" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_InventoryQuantityTotal" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_InventoryLogDetail" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_InventoryCostLogDetail" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_Inventory" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_InventoryCost" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_ItemPrice" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrder_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderPick_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderPack_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderInvoice_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM BASE_Product" + where
    cursor.execute(query, chislo)
    cnxn.commit()

def addProduct(currency, name, price_UAH  = 0, price_EUR = 0, price_USD = 0):

     # стровення нового товару
    insertQuery = """
INSERT INTO BASE_Product 
(Name, Version, ItemType, Description, Remarks, 
BarCode, CategoryId, DefaultLocationId, DefaultSublocation, ReorderPoint, 
ReorderQuantity, Uom, MasterPackQty, InnerPackQty, CaseLength, 
CaseWidth, CaseHeight, CaseWeight, ProductLength, ProductWidth, 
ProductHeight, ProductWeight, Custom1, Custom2, Custom3, 
Custom4, Custom5, ItemTaxCodeId, LastVendorId, IsSellable, 
IsPurchaseable, DateIntroduced, DateUpdated, LastModUserId, LastModDttm, 
Timestamp, IsActive, Custom6, Custom7, Custom8, 
Custom9, Custom10, PictureFileAttachmentId, SoUomName, SoUomRatioStd, 
SoUomRatio, PoUomName, PoUomRatioStd, PoUomRatio)
VALUES 
(?, 1, 1, '', '',
 '', 100, Null, '', Null, 
 Null, '', Null, Null, Null, 
 Null, Null, Null, Null, Null, 
 Null, Null, '', '', '', 
 '', '', 101, Null, 1, 
 1, GETDATE(), Null, 100, GETDATE(),
 DEFAULT, 1, '', '', '', 
 '', '', Null, '', 1, 
 1, '', 1, 1)
"""#  GETDATE(), DEFAULT

    if DEBUG:
        print(insertQuery)
        print(name)

    cursor.execute(insertQuery, name)
    cnxn.commit()

    if DEBUG:
        print('створення товару вдалось')

    # возврат ид
    cursor.execute("SELECT @@IDENTITY")
    newId = cursor.fetchone()[0]

#     #запис інформації про кількість товару
#     stockQuery = """
# INSERT INTO BASE_InventoryQuantityTotal
# (QuantityOnHand, QuantitySold, QuantityOnOrder, Timestamp, ProdId)
# VALUES (?, 0, 0, DEFAULT, ?)
# """
#     cursor.execute(stockQuery, (stock, newId))
#     cnxn.commit()
#
#     stockQuery1 = """
# INSERT INTO BASE_Inventory
# (Quantity, LocationId, Sublocation, Timestamp, ProdId)
# VALUES (?, 100, '', DEFAULT, ?)
#     """
#     cursor.execute(stockQuery1, (stock, newId))
#     cnxn.commit()
#
#     if DEBUG:
#         print('кількість товару добавлена')

    # запис інформації про ціну товару
    priseQuery = """
    INSERT INTO BASE_ItemPrice 
    (UnitPrice, PricingSchemeId, Version, LastModUserId, LastModDttm, Timestamp, ProdId)
    VALUES (?, ?, 1, 100, GETDATE(), DEFAULT, ?)
    """
    if price_UAH:
        cursor.execute(priseQuery, (price_UAH, currency['UAH'], newId))
        cnxn.commit()
    if price_EUR:
        cursor.execute(priseQuery, (price_EUR, currency['EUR'], newId))
        cnxn.commit()
    if price_USD:
        cursor.execute(priseQuery, (price_USD, currency['USD'], newId))
        cnxn.commit()

    if DEBUG:
        print('ціна товару добавлена')

    return newId

def updateProduct(prodId, currency, name, price_UAH, price_EUR, price_USD, pricesDB):

    updateQuery = """
UPDATE BASE_Product 
SET Name = ? 
WHERE ProdId = ?;
"""

    if DEBUG:
        print(updateQuery)
        print(name, prodId)

    cursor.execute(updateQuery, (name, prodId))
    cnxn.commit()

    # оновлення інформації про ціну товару
    updatePriceQuery = """
UPDATE BASE_ItemPrice 
SET UnitPrice = ?
WHERE PricingSchemeId = ? and ProdId = ?;
"""
    insertPriseQuery = """
INSERT INTO BASE_ItemPrice 
(UnitPrice, PricingSchemeId, Version, LastModUserId, LastModDttm, Timestamp, ProdId)
VALUES (?, ?, 1, 100, GETDATE(), DEFAULT, ?)
"""

    for priceDB in pricesDB:
        if priceDB[1] == currency['UAH']:
            if float(priceDB[0]) != 0:
                if DEBUG:
                    print(updatePriceQuery)
                    print(price_UAH, currency['UAH'], prodId)
                cursor.execute(updatePriceQuery, (price_UAH, currency['UAH'], prodId))
                cnxn.commit()
            else:
                if DEBUG:
                    print(insertPriseQuery)
                    print(price_UAH, currency['UAH'], prodId)
                cursor.execute(insertPriseQuery, (price_UAH, currency['UAH'], prodId))
                cnxn.commit()

        if priceDB[1] == currency['EUR']:
            if float(priceDB[0]) != 0:
                if DEBUG:
                    print(updatePriceQuery)
                    print(price_EUR, currency['EUR'], prodId)
                cursor.execute(updatePriceQuery, (price_EUR, currency['EUR'], prodId))
                cnxn.commit()
            else:
                if DEBUG:
                    print(insertPriseQuery)
                    print(price_EUR, currency['EUR'], prodId)
                cursor.execute(insertPriseQuery, (price_EUR, currency['EUR'], prodId))
                cnxn.commit()

        if priceDB[1] == currency['USD']:
            if float(priceDB[0]) != 0:
                if DEBUG:
                    print(updatePriceQuery)
                    print(price_USD, currency['USD'], prodId)
                cursor.execute(updatePriceQuery, (price_USD, currency['USD'], prodId))
                cnxn.commit()
            else:
                if DEBUG:
                    print(insertPriseQuery)
                    print(price_USD, currency['USD'], prodId)
                cursor.execute(insertPriseQuery, (price_USD, currency['USD'], prodId))
                cnxn.commit()


    if DEBUG:
        print('оновлення вдалось')

def ProductSyn(jsonProduct, productsRelationship, update = True):

    # зчитування товарів з БД
    cursor.execute("""
SELECT BASE_Product.ProdId, BASE_Product.Name, BASE_InventoryQuantityTotal.QuantityOnHand 
FROM BASE_Product
LEFT JOIN BASE_InventoryQuantityTotal ON BASE_Product.ProdId = BASE_InventoryQuantityTotal.ProdId;
""")#
    productsDB = cursor.fetchall()
    # pprint(productsDB)

    # зчитування валют з бази
    currency = currencyDB()# currency['UAH'] currency['EUR'] currency['USD']

    if DEBUG:
        pprint(currency)

    # флаги, змінні
    createFlag = True
    newProdId = 0

    # перевірка на співпадання id і ящо ні то перевірка телефона
    for productDB in productsDB:
        if int(jsonProduct['inFlowID']) == int(productDB[0]) and update:  # провірка на співпадання id
            createFlag = False
            # провірка не співпадання хоть одного із параметрів для оновлення
            updateFlag = False

            # зчитування цін
            cursor.execute("""
            SELECT UnitPrice, PricingSchemeId
            FROM BASE_ItemPrice
            WHERE ProdId = ?
            """, jsonProduct['inFlowID'])  #
            pricesDB = cursor.fetchall()
            # pprint(pricesDB)

            for priceDB in pricesDB:
                if priceDB[1] == currency['UAH']:
                    if float(priceDB[0]) != jsonProduct['price_UAH']:
                        updateFlag = True
                if priceDB[1] == currency['EUR']:
                    if float(priceDB[0]) != jsonProduct['price_EUR']:
                        updateFlag = True
                if priceDB[1] == currency['USD']:
                    if float(priceDB[0]) != jsonProduct['price_USD']:
                        updateFlag = True
            if DEBUG:
                print('Обновляю?')
            if updateFlag or jsonProduct['name'] != productDB[1]:
                if LOGING:
                    print('Обновляю товар'
                          + ' ID - ' + str(productDB[0])+'\r\n'
                          + ' Імя - ' + jsonProduct['name']+'\r\n'
                          + ' цена грн - ' + str(jsonProduct['price_UAH'])+'\r\n'
                          + ' цена евро - ' + str(jsonProduct['price_EUR'])+'\r\n'
                          + ' цена долар - ' + str(jsonProduct['price_USD'])+'\r\n'
                          )
                if DEBUG:
                    print('обновляю товар')

                # оновлення товару
                updateProduct(productDB[0], currency, jsonProduct['name'], jsonProduct['price_UAH'],
                              jsonProduct['price_EUR'], jsonProduct['price_USD'], pricesDB)
        else:
            # print(productDB[1].strip(), ' == ', jsonProduct['name'].strip())
            if productDB[1].lower().strip() == jsonProduct['name'].lower().strip():
                if LOGING or DEBUG:
                    print('Товар с таким именем уже существует. Ид - ' + str(productDB[0]))
                productsRelationship.append({jsonProduct['id']: str(productDB[0])})
                return productsRelationship

    if createFlag:
        # Створення нового товара
        if LOGING:
            print('Создаю товар:\r\n'
                  + ' Імя - ' + jsonProduct['name'] + '\r\n'
                  + ' цена грн - ' + str(jsonProduct['price_UAH']) + '\r\n'
                  + ' цена евро - ' + str(jsonProduct['price_EUR']) + '\r\n'
                  + ' цена долар - ' + str(jsonProduct['price_USD']) + '\r\n'
                  )
        if DEBUG:
            print('Создаю товар')
        newProdId = addProduct(currency, jsonProduct['name'], jsonProduct['price_UAH'], jsonProduct['price_EUR'], jsonProduct['price_USD'])

        if DEBUG or LOGING:
            print('---------------------------------------------')

    # запис сворено ид для зворотнього звязку
    if newProdId:
        productsRelationship.append({jsonProduct['id']: str(newProdId)})

    return productsRelationship


def delOrsder(chislo = 0):

    if chislo != 0:
        where = ' WHERE SalesOrderId = ?'
    else:
        where = ' WHERE SalesOrderId > ?'
        chislo = 102

    query = "SELECT SalesOrderShipLineId FROM SO_SalesOrderShip_Line" + where
    cursor.execute(query, chislo)
    conteiners = cursor.fetchall()

    for conteiner in conteiners:
        query = "DELETE FROM SO_SalesOrderShipContainer WHERE SalesOrderShipLineId = ?"
        cursor.execute(query, conteiner[0])
        cnxn.commit()

    query = "DELETE FROM SO_SalesOrderShip_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderPick_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderPack_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrderInvoice_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrder_Line" + where
    cursor.execute(query, chislo)
    cnxn.commit()

    query = "DELETE FROM SO_SalesOrder" + where
    cursor.execute(query, chislo)
    cnxn.commit()

def addOrder(jsonOrsder, curentOrderItems, currency, pricingScheme):

    # зчитування результатів з БД
    cursor.execute("SELECT TOP 1 OrderNumber FROM SO_SalesOrder ORDER BY OrderNumber DESC")
    SO_number = cursor.fetchone()
    if SO_number != None:
        SO_number = SO_number[0]
        number = int(SO_number[-6:])
        number = number + 1
        SO_number = SO_number[:3] + str(number).zfill(6)
    else:
        SO_number = 'SO-000000'

    # стровення нового товару
    insertQuery = """
    INSERT INTO SO_SalesOrder 
    (Version, OrderStatus, OrderNumber, OrderDate, CustomerId, 
    SalesRep, PONumber, RequestShipDate, PaymentTermsId, DueDate, 
    CalculatedDueDate, PricingSchemeId, OrderRemarks, OrderSubTotal, OrderTax1, 
    OrderTax2, OrderExtra, OrderTotal, OrderTaxCodeId, Tax1Rate, 
    Tax2Rate, CalculateTax2OnTax1, Tax1Name, Tax2Name, Email, 
    PickedDate, PickingRemarks, PackedDate, PackingRemarks, ShippingRemarks, 
    IsStandAloneInvoice, InvoicedDate, InvoiceRemarks, InvoiceSubTotal, InvoiceTax1, 
    InvoiceTax2, InvoiceExtra, InvoiceTotal, AmountPaid, InvoiceBalance, 
    ReturnDate, ReturnRemarks, ReturnSubTotal, ReturnTax1, ReturnTax2, 
    ReturnExtra, ReturnTotal, ReturnFee, ReturnRefunded, ReturnCredit, 
    ReturnInventoryBatchLogId, RestockRemarks, ContactName, Phone, BillingAddress1, 
    BillingAddress2, BillingCity, BillingState, BillingCountry, BillingPostalCode, 
    BillingAddressRemarks, BillingAddressType, ShippingAddress1, ShippingAddress2, ShippingCity, 
    ShippingState, ShippingCountry, ShippingPostalCode, ShippingAddressRemarks, ShippingAddressType, 
    Custom1, Custom2, Custom3, Custom4, Custom5, 
    LastModUserId, LastModDttm, Timestamp, AutoInvoice, ParentSalesOrderId, 
    SplitPartNumber, TaxOnShipping, LocationId, IsFullWorkflow, ShowShipping, 
    ShipToCompanyName, CurrencyId, ExchangeRate)
    VALUES 
    (1, 2, ?, ?, ?, 
    '', '', Null, Null, Null, 
    1, ?, ?, ?, 0, 
    0, Null, ?, 100, 0, 
    0, 0, '', '', ?, 
    Null, '', Null, '', '', 
    0, Null, '', 0, 0, 
    0, Null,0, 0, 0, 
    Null, '', 0, 0, 0, 
    Null, 0, 0, 0, 0, 
    Null, '', ?, ?, ?, 
    '', '', '', '', '', 
    '', Null, '', '', '', 
    '', '', '', '', Null, 
    '', '', '', '', '', 
    100, GETDATE(), DEFAULT, 1, Null, 
    Null, 0, Null, 0, 0, 
    '', ?, 1)
    """  # GETDATE(), DEFAULT
    if DEBUG:
        print(insertQuery)

    cursor.execute(insertQuery, (
        SO_number,
        datetime.strptime(jsonOrsder['date'], '%Y-%m-%d'),
        jsonOrsder['customer']['inFlow_user_id'],
        int(pricingScheme),
        str(jsonOrsder['remarks']),
        jsonOrsder['total_price'],
        jsonOrsder['total_price'],
        str(jsonOrsder['email']),
        str(jsonOrsder['customer_name']),
        str(jsonOrsder['phone']),
        str(jsonOrsder['address']),
        int(currency)
    ))
    cnxn.commit()

    if DEBUG:
        print('створення ордеру вдалось')

    # возврат ид
    cursor.execute("SELECT @@IDENTITY")
    newId = cursor.fetchone()[0]


    # запис інформації про товари ордеру
    productsQuery = """
        INSERT INTO SO_SalesOrder_Line 
        (SalesOrderId, Version, LineNum, Description, Quantity, 
        UnitPrice, Discount, SubTotal, ItemTaxCodeId, Timestamp, 
        QuantityUom, QuantityDisplay, DiscountIsPercent, ProdId)
        VALUES 
        (?, 1, ?, '', ?,
        ?, 0, ?, 101, DEFAULT,
        '', ?, 1, ?)
        """#GETDATE(), DEFAULT,
    i = 0
    for curentOrderItem in curentOrderItems:
        i += 1
        if DEBUG:
            print(productsQuery)
            print(newId, i, curentOrderItem['amount'], curentOrderItem['amount'], curentOrderItem['inFlowID'])
        cursor.execute(productsQuery, (newId, i, curentOrderItem['amount'], curentOrderItem['price'],
                                       curentOrderItem['price'], curentOrderItem['amount'], curentOrderItem['inFlowID']))
        cnxn.commit()


    if DEBUG:
        print('товар ордера добавлений')

    return newId

def OrsderSyn(jsonOrsder, orsdersRelationship, jsonOrsdersItems):

    # змінні
    newOrderId = 0

    # зчитування ордерів з БД
    cursor.execute("""
    SELECT SalesOrderId, OrderDate, OrderTotal, CustomerId
    FROM SO_SalesOrder
    """)  #
    ordersDB = cursor.fetchall()
    # pprint(ordersDB)

    # перевірка на співпадання id
    for orderDB in ordersDB:
        if int(jsonOrsder['inFlowOrderId']) == int(orderDB[0]):  # провірка на співпадання id
            return orsdersRelationship

    # зчитування валют з бази
    pricingScheme = currencyDB()  # pricingScheme['UAH'] pricingScheme['EUR'] pricingScheme['USD']
    currency = orderCurrencyDB()  # currency['UAH'] currency['EUR'] currency['USD']
    if DEBUG:
        pprint(currency)

    # визначення валюти ордера
    if jsonOrsder['currency'] == 'UAH':
        currency = currency['UAH']
    if jsonOrsder['currency'] == 'EUR':
        currency = currency['EUR']
    if jsonOrsder['currency'] == 'USD':
        currency = currency['USD']

    # визначення ценової схеми ордера
    if jsonOrsder['currency'] == 'UAH':
        pricingScheme = pricingScheme['UAH']
    if jsonOrsder['currency'] == 'EUR':
        pricingScheme = pricingScheme['EUR']
    if jsonOrsder['currency'] == 'USD':
        pricingScheme = pricingScheme['USD']

    # визначення товарів ордера
    curentOrderItems = []
    for jsonOrsderItems in jsonOrsdersItems:
        if jsonOrsder['id'] == jsonOrsderItems['order_id']:
            curentOrderItems.append(jsonOrsderItems)
    # pprint(curentOrderItems)

    # визначення або створення пользователя
    cutomerId = []
    cutomerId = CustomerSyn(jsonOrsder['customer'], cutomerId, False)[0]
    # pprint(dir(cutomerId))
    cutomerId = cutomerId.popitem()[1]
    jsonOrsder['customer']['inFlow_user_id'] = int(cutomerId)

    # визначення або створення товару
    for curentOrderItem in curentOrderItems:
        # визначення валюти
        if jsonOrsder['currency'] == 'UAH':
            curentOrderItem['price_UAH'] = curentOrderItem['price']
        if jsonOrsder['currency'] == 'EUR':
            curentOrderItem['price_EUR'] = curentOrderItem['price']
        if jsonOrsder['currency'] == 'USD':
            curentOrderItem['price_USD'] = curentOrderItem['price']
        productId = []
        productId = ProductSyn(curentOrderItem, productId, False)[0]
        productId = productId.popitem()[1]
        curentOrderItem['inFlowID'] = int(productId)



    # перевірка на співпадання id і ящо ні то перевірка телефона
    for orderDB in ordersDB:
        if int(jsonOrsder['inFlowOrderId']) == int(orderDB[0]):  # провірка на співпадання id
            return orsdersRelationship
        else:
            orderProductsFlag = False
            # зчитування товарів ордера з БД
            cursor.execute("SELECT ProdId, Quantity FROM SO_SalesOrder_Line WHERE SalesOrderId = ?", orderDB[0])  #
            orderItemsDB = cursor.fetchall()

            # print(len(orderItemsDB), ' == ', len(curentOrderItems),len(orderItemsDB) == len(curentOrderItems))
            if len(orderItemsDB) == len(curentOrderItems):
                itemsCount = 0
                for curentOrderItem in curentOrderItems:
                    orderProductFlag = False
                    for orderItemDB in orderItemsDB:
                        if curentOrderItem['inFlowID'] == int(orderItemDB[0]) and curentOrderItem['amount'] == float(orderItemDB[1]):
                            orderProductFlag = True
                    if orderProductFlag:
                        itemsCount += 1
                if itemsCount == len(orderItemsDB):
                    orderProductsFlag = True

            # print(jsonOrsder['date'] + '==' + orderDB[1].strftime("%Y-%m-%d"), jsonOrsder['date'] == orderDB[1].strftime("%Y-%m-%d"))
            # print(str(jsonOrsder['total_price']) + '==' + str(orderDB[2]), jsonOrsder['total_price'] == float(orderDB[2]))
            # print(str(jsonOrsder['customer']['inFlow_user_id']) + '==' + str(orderDB[3]), int(jsonOrsder['customer']['inFlow_user_id']) == int(orderDB[3]))
            if jsonOrsder['date'] == orderDB[1].strftime("%Y-%m-%d") and jsonOrsder['total_price'] == float(orderDB[2]) and \
                    int(jsonOrsder['customer']['inFlow_user_id']) == int(orderDB[3]) and orderProductsFlag:
                if LOGING or DEBUG:
                    pprint('Этот ордер уже существует. Ид - ' + str(orderDB[0]))
                orsdersRelationship.append({jsonOrsder['id']: str(orderDB[0])})
                return orsdersRelationship
           #провірка нема лі вже созданих ключів які б мішали создати такий ордер, еслі є то ретрн з ид


    # Створення нового ордера
    if LOGING:
        print('Создаю ордер:\r\n'
              + 'Дата - ' + jsonOrsder['date'] + '\r\n'
              + 'валюта - ' + jsonOrsder['currency'] + '\r\n'
              + 'загальна сума - ' + str(jsonOrsder['total_price']) + '\r\n'
              + 'Ід покупця - ' + str(jsonOrsder['customer']['inFlow_user_id']) + '\r\n'
              + 'Імя покупця - ' + jsonOrsder['customer']['name'] + '\r\n'
              + 'Список товарів:'
              )
        for curentOrderItem in curentOrderItems:
            print('Ід - ' + str(curentOrderItem['inFlowID']) + '\t'
                  + ' назва - ' + curentOrderItem['name'] + '\t\t'
                  + ' кількість - ' + str(curentOrderItem['amount']) + '\t'
                  + ' ціна - ' + str(curentOrderItem['price'])
                  )
    if DEBUG:
        print('Создаю заказ')
    newOrderId = addOrder(jsonOrsder, curentOrderItems, currency, pricingScheme)

    # запис сворено ид для зворотнього звязку
    if newOrderId:
        orsdersRelationship.append({jsonOrsder['id']: str(newOrderId)})
    if DEBUG or LOGING:
        print('---------------------------------------------')

    return orsdersRelationship

#functions--------------------------------------------------------------------------------------------------------------


#start_point------------------------------------------------------------------------------------------------------------
# inputForm = {'url': 'http://192.168.1.6/inFlowExport.php',
#              'DB': {'server': 'КОМП_ВАДИМА\INFLOWSQL',
#                     'database': 'inFlow',
#                     'username': 'MySA',
#                     'password': '1234'
#                    }
#              }
inputForm = {'url': 'https://test.cnd.com.ua/inFlowExport.php',
             'DB_server': 'КОМП_ВАДИМА',
             'currency': {
                 'UAH': 'UAH',
                 'EUR': 'EUR',
                 'USD': 'USD'
             }
             }

try:
    f = open('settings.ini')
except IOError as e:
    with open('settings.ini', 'w') as outfile:
        json.dump(inputForm, outfile, sort_keys=True, indent=2)#, ensure_ascii=False
    print('Заповніть файл settings.ini настройками.')
    os.system("pause")
    exit(0)
else:
    # зчитування json файла
    try:
        settings = json.load(f)
    except Exception as e:
        print('Проблема с json кодировкой, попробуйте закодировать строки на кирилеце, перед \ поставить еще 1.')
    f.close()

    # pprint(settings)

url = ''
server = ''
database = ''
username = ''
password = ''
currencyNames = {}

if '-l' in sys.argv:
    LOGING = True

if '-d' in sys.argv:
    DEBUG = True

url = settings['url']

currencyNames['UAH'] = settings['currency']['UAH']
currencyNames['EUR'] = settings['currency']['EUR']
currencyNames['USD'] = settings['currency']['USD']

server = settings['DB_server']

# server = settings['DB']['server']
# database = settings['DB']['database']
# username = settings['DB']['username']
# password = settings['DB']['password']


# url = 'http://192.168.1.6/inFlowExport.php'
# server = 'КОМП_ВАДИМА\INFLOWSQL'
# database = 'inFlow'
# username = 'MySA'
# password = '1234'

if url == '' or server == '':
    print('Заповніть файл settings.ini настройками.')
    os.system("pause")
    exit(0)


# відправка запроса на сайт
headers = {'Content-type': 'application/json',
           'tokenn': 'dd5230f6e453c712630e',
           'Accept': 'text/plain',
           'Content-Encoding': 'utf-8'}

answer = requests.get(url, headers=headers)
# print(url,headers)
# print(answer)
if answer:
    jsonData = answer.json()
    # pprint(jsonData)

#зчитування json файла
# with open('results.json') as f:
#     jsonData = json.load(f)

jsonCustomers = jsonData['customers']
jsonProducts = jsonData['products']
jsonOrsders = jsonData['orsders']
jsonOrsdersItems = jsonData['orsdersItems']

if LOGING:
    print('Дані для синхронізації получені.')

# нормалізація масіва пользователів
for jsonCustomer in jsonCustomers:
    # jsonProduct['id']
    # jsonProduct['inFlow_user_id']
    jsonCustomer['email'] = str(jsonCustomer['email']).strip()
    jsonCustomer['name'] = str(jsonCustomer['name']).strip()
    jsonCustomer['phone'] = str(jsonCustomer['phone']).strip()
    jsonCustomer['address'] = str(jsonCustomer['address']).strip()
    if jsonCustomer['discount'] == None:
        jsonCustomer['discount'] = 0
    jsonCustomer['discount'] = float(jsonCustomer['discount'])

# нормалізація масіва товарів
for jsonProduct in jsonProducts:
    # jsonProduct['id']
    # jsonProduct['inFlowID']
    jsonProduct['name'] = str(jsonProduct['name']).strip() + ' ' + str(jsonProduct['variant']).strip()
    jsonProduct['price_UAH'] = float(jsonProduct['price_UAH'])
    jsonProduct['price_USD'] = float(jsonProduct['price_USD'])
    jsonProduct['price_EUR'] = float(jsonProduct['price_EUR'])

# нормалізація масіва ордерів
for jsonOrsder in jsonOrsders:
    if jsonOrsder['id'] == None:
        jsonOrsder['id'] = 0
    else:
        jsonOrsder['id'] = int(jsonOrsder['id'])

    if jsonOrsder['inFlowOrderId'] == None:
        jsonOrsder['inFlowOrderId'] = 0
    else:
        jsonOrsder['inFlowOrderId'] = int(jsonOrsder['inFlowOrderId'])

    jsonOrsder['date'] = str(jsonOrsder['date']).strip()
    jsonOrsder['date'] = jsonOrsder['date'].split(' ')[0]

    jsonOrsder['currency'] = str(jsonOrsder['currency']).strip() #'UAH', 'EUR', 'USD'
    jsonOrsder['total_price'] = float(jsonOrsder['total_price'])
    if jsonOrsder['user_id'] == None:
        jsonOrsder['user_id'] = 0
    else:
        jsonOrsder['user_id'] = int(jsonOrsder['user_id'])

    if jsonOrsder['inFlow_user_id'] == None:
        jsonOrsder['inFlow_user_id'] = 0
    else:
        jsonOrsder['inFlow_user_id'] = int(jsonOrsder['inFlow_user_id'])

    jsonOrsder['customer'] = {'id': jsonOrsder['user_id'],
                              'inFlow_user_id': jsonOrsder['inFlow_user_id'],
                              'email': str(jsonOrsder['email']).strip(),
                              'name': str(jsonOrsder['customer_name']).strip(),
                              'phone': str(jsonOrsder['phone']).strip(),
                              'address': str(jsonOrsder['address']).strip(),
                              'discount': 0
                              }
    if jsonOrsder['coupon_discount'] == None:
        jsonOrsder['coupon_discount'] = 0
    else:
        jsonOrsder['coupon_discount'] = float(jsonOrsder['coupon_discount'])
    if jsonOrsder['discount'] == None:
        jsonOrsder['discount'] = 0
    else:
        jsonOrsder['discount'] = float(jsonOrsder['discount'])
    if jsonOrsder['traking'] == None:
        jsonOrsder['traking'] = ''
    if jsonOrsder['note'] == None:
        jsonOrsder['note'] = ''
    if jsonOrsder['delivery_price'] == None:
        jsonOrsder['delivery_price'] = 0
    else:
        jsonOrsder['delivery_price'] = float(jsonOrsder['delivery_price'])

    jsonOrsder['remarks'] = ''
    if jsonOrsder['discount'] != 0:
        jsonOrsder['remarks'] += 'Скидка ' + str(jsonOrsder['discount']) + '\r\n'
    if jsonOrsder['coupon_discount'] != 0:
        jsonOrsder['remarks'] += 'Купон ' + str(jsonOrsder['coupon_discount']) + '\r\n'
    if (jsonOrsder['discount'] != 0) or (jsonOrsder['coupon_discount'] != 0):
        jsonOrsder['remarks'] += '\r\n'
    jsonOrsder['remarks'] += 'Номер интернет заказа №' + str(jsonOrsder['id']) + '\r\n'
    if jsonOrsder['delivery_price'] != 0:
        jsonOrsder['remarks'] += 'Цена доставки ' + str(jsonOrsder['delivery_price']) + '\r\n'
    if jsonOrsder['traking'] != '':
        jsonOrsder['remarks'] += 'Номер посылки ' + jsonOrsder['traking'] + '\r\n'
    if jsonOrsder['note'] != '':
        jsonOrsder['remarks'] += 'Примечание ' + jsonOrsder['note'] + '\r\n'

# нормалізація масіва товарів для ордерів
for jsonOrsderItem in jsonOrsdersItems:
    if jsonOrsderItem['order_id'] == None:
        jsonOrsderItem['order_id'] = 0
    else:
        jsonOrsderItem['order_id'] = int(jsonOrsderItem['order_id'])

    if jsonOrsderItem['inFlowID'] == None:
        jsonOrsderItem['inFlowID'] = 0
    else:
        jsonOrsderItem['inFlowID'] = int(jsonOrsderItem['inFlowID'])

    if jsonOrsderItem['variant_id'] == None:
        jsonOrsderItem['variant_id'] = 0
    else:
        jsonOrsderItem['variant_id'] = int(jsonOrsderItem['variant_id'])

    if jsonOrsderItem['amount'] == None:
        jsonOrsderItem['amount'] = 0
    else:
        jsonOrsderItem['amount'] = float(jsonOrsderItem['amount'])

    if jsonOrsderItem['price'] == None:
        jsonOrsderItem['price'] = 0
    else:
        jsonOrsderItem['price'] = float(jsonOrsderItem['price'])

    jsonOrsderItem['name'] = str(jsonOrsderItem['product_name']).strip() + ' ' + str(jsonOrsderItem['variant_name']).strip()
    jsonOrsderItem['price_UAH'] = 0
    jsonOrsderItem['price_EUR'] = 0
    jsonOrsderItem['price_USD'] = 0
    jsonOrsderItem['id'] = 0



# pprint(jsonOrsders)
# exit()

#підключення до БД
#cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+'\INFLOWSQL;DATABASE=inFlow;Trusted_Connection=Yes;')
cursor = cnxn.cursor()

#оновлення покупців-----------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------

#масив зворотнього звязку покупців
customersRelationship = []
if LOGING:
    print('\r\n\r\nСинхронізація покупців:\r\n')
for jsonCustomer in jsonCustomers:
    if 1:  # перемикач якщо потрібно зачистити пользователів
        customersRelationship = CustomerSyn(jsonCustomer, customersRelationship)
    else:
        delCutomer()
        exit(0)

#-----------------------------------------------------------------------------------------------------------------------
#оновлення покупців-----------------------------------------------------------------------------------------------------

#оновлення товарів------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
productsRelationship = []
if LOGING:
    print('\r\n\r\nСинхронізація товарів:\r\n')
for jsonProduct in jsonProducts:
    if 1:# перемикач якщо потрібно зачистити товари
        productsRelationship = ProductSyn(jsonProduct, productsRelationship)
    else:
        delProduct()
        exit(0)

#-----------------------------------------------------------------------------------------------------------------------
#оновлення товарів------------------------------------------------------------------------------------------------------

#оновлення заказів------------------------------------------------------------------------------------------------------
#-----------------------------------------------------------------------------------------------------------------------
orsdersRelationship = []
if LOGING:
    print('\r\n\r\nСинхронізація заказів:\r\n')
for jsonOrsder in jsonOrsders:
    if 1:# перемикач якщо потрібно зачистити ордера
        orsdersRelationship = OrsderSyn(jsonOrsder, orsdersRelationship, jsonOrsdersItems)
    else:
        delOrsder()
        exit(0)

#-----------------------------------------------------------------------------------------------------------------------
#оновлення заказів------------------------------------------------------------------------------------------------------

# вивод обратної связі
response = {}
#покупці
if customersRelationship:
    response['customers'] = customersRelationship
#товари
if productsRelationship:
    response['products'] = productsRelationship
#закази
if orsdersRelationship:
    response['orders'] = orsdersRelationship
if response:
    pprint(response)

# передача відповіді
requests.post(url, data=json.dumps(response), headers=headers)

#закриття з'єднання з БД
cursor.close()
del cursor

# print('провірити валюти в реальній базі')
# print('провірити TaxCodeId в реальній базі 101 - без налогів')

if DEBUG or '-p' in sys.argv:
    os.system("pause")



