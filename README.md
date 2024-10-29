this is the output from below

(<function <lambda> at 0x7f601fbbf240>, 'https://customer-experience-api.arifleet.com/v1/billing?billingTypeCode=1&pageNumber=1')

fetch_data_udf = udf(lambda url: fetch_data_from_url(url)),(single_url)
print(fetch_data_udf)
