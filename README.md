fetch_data_udf = udf(lambda url: fetch_data_from_url(url), StringType())
url = "https://customer-experience-api.arifleet.com/v1/billing?billingTypeCode=1&pageNumber=1" 
data = fetch_data_from_url(url) 
print("Data from single URL test:", data)



Data from single URL test: {"pageNumber":"1","totalPages":"1133","billing":[{"accountCode":"198728","amountBilled":"0.44","description":" JE- 1624208 CO 1084489600","division":"SL","dueDate":"09/15/2024","invoiceNumber":"18039317","items":"TOLL MGMT PROGRAM FEE","lesseeCode":"5DA4","billLesseeData":"3600","odometer":"118291","poNumber":"","prefix":"5310","referenceDate":"07/26/2024","invoiceDate":"08/26/2024","ataCode":"","vehicleNumber":"7R094","vendorInvoiceNumber":"","vendorName":"","lesseeData2":"","assetType":"TRUCK MD","auxData1":"350","auxData10":"DENVER","auxData11":"FAB METALS","auxData12":"175","auxData13":"0","auxData14":"","auxData2":"RESIDENTIAL LAWN MEDIUM","auxData3":"","auxData4":"","auxData5":"0","auxData6":"","auxData7":"ISUZU SHELL","auxData8":"","auxData9":"LAWN","clientVehicleNumber":"7R094","firstName":"BRANCH","leaseTerm":"8
