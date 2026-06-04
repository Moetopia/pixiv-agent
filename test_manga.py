import sys, os
import json
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from app.pixiv_client import get_api

def test():
    api = get_api()
    res = api.illust_detail(137189980)
    illust = res.get("illust", {})
    user_id = illust.get("user", {}).get("id")
    print(f"Author for 137189980: {user_id}")
    
    if user_id:
        print("Fetching type=manga...")
        res_manga = api.user_illusts(user_id, type="manga")
        types_manga = set(i.get("type") for i in res_manga.get("illusts", []))
        print(f"manga Types returned: {types_manga}")

        print("Fetching type=illust...")
        res_illust = api.user_illusts(user_id, type="illust")
        types_illust = set(i.get("type") for i in res_illust.get("illusts", []))
        print(f"illust Types returned: {types_illust}")

if __name__ == "__main__":
    test()
