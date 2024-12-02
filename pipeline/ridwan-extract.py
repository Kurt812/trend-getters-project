from pytrends.request import TrendReq

# Only need to run this once, the rest of requests will use the same session.
pytrend = TrendReq()

# Create payload and capture API tokens. Only needed for interest_over_time(), interest_by_region() & related_queries()
topics = ['pizza']
pytrend.build_payload(kw_list=topics)

# Interest Over Time
print("Interest over time")
interest_over_time_df = pytrend.interest_over_time()
print(interest_over_time_df.head())

# Interest by Region
print("Interest by region")
interest_by_region_df = pytrend.interest_by_region()
print(interest_by_region_df.head())

# Related Queries, returns a dictionary of dataframes
# related_queries_dict = pytrend.related_queries()
# print(related_queries_dict)


# Get Google Keyword Suggestions
print("Suggestions for Pizza")
suggestions_dict = pytrend.suggestions(keyword='pizza')
print(suggestions_dict)


# Recreate payload with multiple timeframes
pytrend.build_payload(kw_list=['pizza', 'bagel'], timeframe=[
                      '2022-09-04 2022-09-10', '2022-09-18 2022-09-24'])

# Multirange Interest Over Time
print("Interest of pizza and bagel from 2022-09-04 to 2022-09-10 ")
multirange_interest_over_time_df = pytrend.multirange_interest_over_time()
print(multirange_interest_over_time_df.head())
