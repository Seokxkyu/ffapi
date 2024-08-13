from typing import Union

from fastapi import FastAPI, HTTPException
import pandas as pd
import os
import requests

app = FastAPI()

# global variable
df = pd.read_parquet("/home/kyuseok00/code/ffapi/data")

@app.get("/")
def read_root():
    return {"Hello": "World"}

def req(movie_cd):
    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/movie/searchMovieInfo.json"
    key = os.getenv('MOVIE_API_KEY')
    url = f"{base_url}?key={key}&movieCd={movie_cd}"
    r = requests.get(url)
    data = r.json()
    l = data['movieInfoResult']['movieInfo']['nations']
    nationNm = l[0].get('nationNm')
    if nationNm == '한국':
        return 'K'
    else:
        return 'F'

@app.get("/sample")
def sample_data():
    df = pd.read_parquet("/home/kyuseok00/code/ffapi/data")
    sample_df = df.sample(n=5)
    r = sample_df.to_dict(orient='records')
    return r

@app.get("/movie/{movie_cd}")
def movie_meta(movie_cd: str):
    # local variable
    # df = pd.read_parquet("/home/kyuseok00/code/ffapi/data")
    # df에서 movieCd == movie_cd인 row를 조회
    meta_df = df[df['movieCd'] == movie_cd]
    
    if meta_df.empty:
        raise HTTPException(status_code=404, detail='영화를 찾을 수 없습니다')
    # 조회된 데이터를 .to_dict()로 만들어 아래에서 return
    r = meta_df.iloc[0].to_dict()
    
    if r.get('repNationCd') is None:
        r['repNationCd'] = req(movie_cd)

    return r
    # return {"movie_cd": movie_cd, "df_count": len(df)}
