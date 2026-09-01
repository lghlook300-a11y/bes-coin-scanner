# BES 실시간 수급 검색기

빗썸 Public WebSocket의 `ticker`, `trade`, `orderbook`을 계속 수신해 정상 KRW 종목의 자금 유입을 탐지합니다. 주문·금액·손절·가상계좌 기능은 없습니다.

## 출력 상태

- `수급 유입`: 평소보다 체결대금과 매수 체결이 증가
- `상승 가능`: 수급·가격 반응·호가 매수 우위가 함께 확인
- `과열·추격 금지`: 이미 단시간 급등
- `수급 이탈`: 매도 체결과 가격 하락이 동시에 확대

## 실행

```bash
pip install -r requirements.txt
python realtime_scanner.py
```

브라우저에서 `http://localhost:8080`을 엽니다. 실시간 운영에는 프로그램이 종료되지 않는 24시간 서버가 필요합니다.
