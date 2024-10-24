def get_total_pages(data_type, code_key, data_key, code, token, batch_size=100):
    response, response_data = get_holman_api_response(token,f"{data_type}?{code_key}={code}&pageNumber=1")
    if 'totalPages' in response_data:
        total_pages = int(response_data['totalPages'])
        return total_pages
    else:
        raise Exception("No totalPages found in first response")

total_pages = get_total_pages("billing", "billingTypeCode", "billing", 3, token)
print(total_pages)



"pageNumber": "24",
    "totalPages": "852",
    "us": [
        {
            "usRecordID": "793195922",
            "lesseeCode": "0D11",
            "ariVehicleNumber": "120187",
            "clientVehicleNumber": "120187",
            "transactionDate": "9/23/2024 5:50:00 PM",
            "wexAccountNumber": "",
            "vendorName": "BP OIL",
            "siteName": "BP#9226333PR",
            "siteAddress": "1905 FRANCISCAN WAY",
            "siteCity": "WEST CHICAGO",
            "siteState": "IL",
            "siteZip": "60185",
            "driverFuelPin": "999622",
            "firstName": "JOHN",
            "lastName": "VENEN",
            "odometer": "32616",
            "product": "UNLEADED",
            "unitCost": "3.507",
            "gallons": "18.9",
            "amount": "67.64",
            "transactionLoadDate": "9/25/2024 12:00:00 AM",
            "transactionString": "20240923175000",
            "division": "TG",
            "prefix": "5806",
            "clientData1": "TG2049",
            "clientData2": "",
            "clientData3": "TG49",
            "clientData4": "9051",
            "clientData5": "9105",
            "clientData6": "WEST CHICAGO",
            "clientData7": "58069999",
            "driverClass": "",
            "exec": "Y",
            "auxData1": "300",
            "auxData2": "RESIDENTIAL HORT",
            "auxData3": "",
            "auxData4": "",
            "auxData5": "100",
            "auxData6": "",
            "auxData7": "",
            "auxData8": "",
            "auxData9": "HORT",
            "auxData10": "",
            "auxData11": "MONROE",
            "auxData12": "200",
            "auxData13": "",
            "auxData14": "",
            "auxDate1": "",
            "auxDate2": "",
            "auxDate3": "",
            "auxDate4": ""
        },
