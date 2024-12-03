from pytrends.request import TrendReq


def initialize_trend_request():
    """Initialize and return a TrendReq object."""
    return TrendReq()


def fetch_interest_over_time(pytrend: TrendReq, topics: list[str]):
    """Fetch and print interest over time for given topics."""
    pytrend.build_payload(kw_list=topics)
    print("Interest over time")
    interest_over_time_df = pytrend.interest_over_time()
    print(interest_over_time_df.head())
    return interest_over_time_df


def fetch_interest_by_region(pytrend: TrendReq, topics: list[str]):
    """Fetch and print interest by region for given topics."""
    pytrend.build_payload(kw_list=topics)
    print("Interest by region")
    interest_by_region_df = pytrend.interest_by_region()
    print(interest_by_region_df.head())
    return interest_by_region_df


def fetch_suggestions(pytrend: TrendReq, keyword: str):
    """Fetch and print suggestions for a given keyword."""
    # print(f"Suggestions for {keyword}")
    suggestions_dict = pytrend.suggestions(keyword=keyword)
    # print(suggestions_dict)
    return suggestions_dict


def fetch_multirange_interest(pytrend: TrendReq, topics: list[str], timeframes: list[str]):
    """Fetch and print multirange interest over time for given topics and timeframes."""
    pytrend.build_payload(kw_list=topics, timeframe=timeframes)
    print(f"Interest of pizza and bagel from 2022-09-04 to 2022-09-10 ")
    multirange_interest_over_time_df = pytrend.multirange_interest_over_time()
    print(multirange_interest_over_time_df.head())
    return multirange_interest_over_time_df


def main(topics: list[str]) -> None:
    pytrend = initialize_trend_request()

    fetch_interest_over_time(pytrend, topics)
    print("")
    fetch_interest_by_region(pytrend, topics)
    print("")
    for keyword in topics:
        fetch_suggestions(pytrend, keyword)

    timeframes = ['2022-09-04 2022-09-10', '2022-09-18 2022-09-24']
    fetch_multirange_interest(pytrend, topics, timeframes)


if __name__ == "__main__":
    main()
