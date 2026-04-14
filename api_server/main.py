'''
- 요구사항
    - 신용 평가~
'''

# 1. 모듈 가져오기
from fastapi import FastAPI
from pydantic import BaseModel
from typing import List
import random

# 2. FastAPI 앱(객체) 생성
app = FastAPI()

# 3. 요청/응답 데이터의 구조를 정의 + 유효선검사 틀을 제공하는 클래스 구성 -> pydantic 사용
class ReqData(BaseModel): #요청
    user_id:str
    income:int
    loan_amt:int
    pass

class ResData(BaseModel): #응답
    user_id:str
    credit_score:int
    grade:str
    pass

# 4. 라우팅 (url 정의, 해당 요청 시 처리 할 함수 매칭)
@app.get('/')
def home():
    return {'status' : 'AI 신용평가 서비스 API'}

@app.post('/predict', response_model=List[ResData])
def predict( users:List[ReqData] ):
    result = list()
    for user in users:
        a = (user.income // 1000) * 10
        credit_score = min(random.randint(300,600)+a, 990)
        grade        = "A" if credit_score >= 800 else "B" if credit_score >= 600 else "C"

        result.append(
            {
                "user_id" : user.user_id,
                "credit_score": credit_score,
                "grade" : grade
            }
        )
        
    return result