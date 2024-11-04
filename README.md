def replace_nulls_in_dataframe(df: DataFrame):
    # return df.replace("null", None).fillna(" ")


def replace_nulls_in_dataframe(df: DataFrame): 
    return df.select([when(col(c).isNull() | (col(c) == "null"), lit(" ")).otherwise(col(c)).alias(c) for c in df.columns])



[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve "(billing = null)" due to data type mismatch: the left and right operands of the binary operator have incompatible types ("ARRAY<STRUCT<accountCode: STRING, amountBilled: STRING, assetType: STRING, ataCode: STRING, auxData1: STRING, auxData10: STRING, auxData11: STRING, auxData12: STRING, auxData13: STRING, auxData14: STRING, auxData2: STRING, auxData3: STRING, auxData4: STRING, auxData5: STRING, auxData6: STRING, auxData7: STRING, auxData8: STRING, auxData9: STRING, billLesseeData: STRING, billingCategory: STRING, bookValue: STRING, capCost: STRING, clientVehicleNumber: STRING, description: STRING, division: STRING, dueDate: STRING, fileNumber: STRING, firstBillIndicator: STRING, firstName: STRING, gstTax: STRING, insuranceAmount: STRING, interest: STRING, invoiceDate: STRING, invoiceNumber: STRING, items: STRING, lastBillIndicator: STRING, lastName: STRING, leaseTerm: STRING, lesseeCode: STRING, lesseeData1: STRING, lesseeData2: STRING, lesseeData3: STRING, lesseeData4: STRING, lesseeData5: STRING, lesseeData6: STRING, lesseeData7: STRING, licensePlate: STRING, makeName: STRING, managementFee: STRING, modelName: STRING, modelYear: STRING, monthsBilled: STRING, monthsInService: STRING, odometer: STRING, poNumber: STRING, prefix: STRING, printDate: STRING, record_id: STRING, referenceDate: STRING, rental: STRING, stateProvince: STRING, taxAmount: STRING, vehicleNumber: STRING, vendorInvoiceNumber: STRING, vendorName: STRING, vin: STRING, yearMonth: STRING>>" and "STRING"). SQLSTATE: 42K09
File <command-2638019235405315>, line 43
