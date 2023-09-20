import pandas as pd


def parseBonus(data: pd.DataFrame):
    print(data)
    bonus_regex1 = "^BONUS (\d):(\d).*"
    t = data["purpose"].str.extract(bonus_regex1)
    print(t)


def parseCorpAction(data: pd.DataFrame):
    parseBonus(data)


def main():
    pass


if __name__ == "__main__":
    main()
