# BES Coin Scanner

업비트 KRW 마켓 전체를 BES V4.9.1 구조 기준으로 검사합니다.

- 확정 일봉 400개 이상
- 확정 4시간봉 450개 이상
- PRE-A, A, CONFIRM, P-FAIL, FAIL
- 일봉 상승 S, 하락 BEAR-S
- 임의 점수·상승률 순위·매수 추천 없음

결과는 `data/latest.json`에 저장됩니다. GitHub Actions는 한국시간 01:10, 05:10, 09:10, 13:10, 17:10, 21:10에 실행됩니다.

업비트 공개 REST API만 사용하므로 API 키와 거래소 계정은 필요하지 않습니다.
