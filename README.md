# Test a single URL directly
single_url = "https://customer-experience-api.arifleet.com/v1/billing?billingTypeCode=1&pageNumber=1"
data = fetch_data_from_url(single_url)
print("Data from single URL test:", data)
