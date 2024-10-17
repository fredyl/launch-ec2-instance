def replace_null_values(item):
    return [
        {key: (value if value is not None else "") for key, value in item.items()}
        for item in item
    ]


 "accountCode": null,
        "amountBilled": "762.22",
        "description": null,
        "division": "TG",
        "dueDate": "10/15/2024",
        "invoiceNumber": "B18041485",
        "items": null,
        "lesseeCode": "0D24",
        "billLesseeData": "MTHLY CHARGE",
        "odometer": null,
        "poNumber": null,
        "prefix": "5049",
        "referenceDate": null,
        "invoiceDate": "09/25/2024",
