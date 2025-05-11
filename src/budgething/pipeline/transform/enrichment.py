from typing import Any


def add_balance(data: list[dict[str, Any]], latest_balance: float) -> list[dict[str, Any]]:
    """Calculate balance after each transaction.

    Args:
        data (list[dict[str, Any]]):
            Entries must include at least date and amount.
        latest_balance (float):
            Balance after the most recent transaction.

    Returns:
        list[dict[str, Any]]: Same as input, but with an additional field `balance`.


    """
    if len(data) == 0:
        return data
    if not all("date" in entry and "amount" in entry for entry in data):
        raise ValueError("Data must contain 'date' and 'amount' fields.")

    sorted_data = sorted(data, key=lambda d: d["date"], reverse=True)

    # Balance after the most recent transaction is the starting point for the balance calculation
    sorted_data[0]["balance"] = latest_balance

    for i, entry in enumerate(sorted_data[1:]):
        entry["balance"] = sorted_data[i]["balance"] - sorted_data[i]["amount"]

    return sorted_data
