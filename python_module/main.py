from collections import defaultdict


def total_revenue(purchases: list[dict]) -> float:
    total = 0
    for purchase in purchases:
        total += purchase["price"] * purchase["quantity"]
    
    return total


def items_by_category(purchases: list[dict]) -> dict[str, list[str]]:
    items = defaultdict(set)
    for purchase in purchases:
        category = purchase["category"]
        item = purchase["item"]

        items[category].add(item)

    for k in items.keys():
        items[k] = list(items[k])
    
    return items


def expensive_purchases(purchases: list[dict], min_price: float) -> list[dict]:
    result = []
    for purchase in purchases:
        if purchase["price"] >= min_price:
            result.append(purchase)
    
    return result


def average_price_by_category(purchases: list[dict]) -> dict[str, float]:
    result = defaultdict(list)
    for purchase in purchases:
        category = purchase["category"]
        price = purchase["price"]

        result[category].append(price)
    
    for category, prices in result.items():
        result[category] = sum(prices) / len(prices)
    
    return result


def most_frequent_category(purchases: list[dict]) -> str:
    result = defaultdict(int)
    for purchase in purchases:
        category = purchase["category"]
        quantity = purchase["quantity"]

        result[category] += quantity
    
    category = max(result, key=result.get)
    
    return category


purchases = [
    {"item": "apple", "category": "fruit", "price": 1.2, "quantity": 10},
    {"item": "banana", "category": "fruit", "price": 0.5, "quantity": 5},
    {"item": "milk", "category": "dairy", "price": 1.5, "quantity": 2},
    {"item": "bread", "category": "bakery", "price": 2.0, "quantity": 3},
]
min_price = 1.0

print(f"Общая выручка: {total_revenue(purchases)}")
print(f"Товары по категориям: {items_by_category(purchases)}")
print(f"Покупки дороже {min_price}: {expensive_purchases(purchases, min_price)}")
print(f"Средняя цена по категориям: {average_price_by_category(purchases)}")
print(f"Категория с наибольшим количеством проданных товаров: {most_frequent_category(purchases)}")
