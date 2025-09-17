from datetime import datetime


def generate_period_codes(start_year: int = 2023, end_year: int = None, end_month: int = None) -> list:
    current_date = datetime.now()
    if end_year is None:
        end_year = current_date.year
    if end_month is None:
        end_month = current_date.month

    period_codes = []

    for year in range(start_year, end_year + 1):
        start_month = 1 if year > start_year else 1
        end_month_for_year = end_month if year == end_year else 12

        for month in range(start_month, end_month_for_year + 1):
            period_code = f"{year}-{month:02d}"
            period_codes.append(period_code)

    return period_codes


if __name__ == '__main__':
    result = generate_period_codes(2023)
    print(result)
