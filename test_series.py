import asyncio
import os
import sys

# Add app to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from app.pixiv_client import get_api

def test_user_illusts():
    api = get_api()
    
    # 找一个已知有漫画/系列的用户，比如 8863640 (比如有些有连载漫画的作者)
    # 或者我们查一下数据库里有没有
    pass

if __name__ == "__main__":
    api = get_api()
    print("Testing without type...")
    res = api.user_illusts(3036836, offset=0) # An author
    print("Length:", len(res.get("illusts", [])))
    if res.get("illusts"):
        print("First illust type:", res["illusts"][0].get("type"))
        
    print("Testing with type=manga...")
    res = api.user_illusts(3036836, type="manga", offset=0)
    print("Length:", len(res.get("illusts", [])))
    if res.get("illusts"):
        print("First illust type:", res["illusts"][0].get("type"))
