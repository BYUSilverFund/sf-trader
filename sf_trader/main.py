from ib_insync import IB

def get_account_value():
    # Create an IB instance
    ib = IB()

    # Connect to TWS or IB Gateway
    # Default ports: TWS live=7496, TWS paper=7497, Gateway live=4001, Gateway paper=4002
    ib.connect('127.0.0.1', 7497, clientId=1)

    # Request account summary
    account_summary = ib.accountSummary()

    # Get account values
    account_values = ib.accountValues()

    print("\n=== Account Summary ===")
    for item in account_summary:
        if item.tag in ['NetLiquidation', 'TotalCashValue', 'BuyingPower', 'GrossPositionValue']:
            print(f"{item.tag}: {item.value} {item.currency}")

    print("\n=== Key Account Values ===")
    for value in account_values:
        if value.tag in ['NetLiquidation', 'TotalCashValue', 'BuyingPower', 'AvailableFunds']:
            print(f"{value.tag}: {value.value} {value.currency} (Account: {value.account})")

    # Get specific net liquidation value
    net_liquidation = [v for v in account_values if v.tag == 'NetLiquidation']
    if net_liquidation:
        print(f"\n=== Total Account Value ===")
        for nl in net_liquidation:
            print(f"Account {nl.account}: {nl.value} {nl.currency}")

    ib.disconnect()

if __name__ == "__main__":
    get_account_value()
