# bundis 성능 벤치마크 계획 (rev 2 — 방법론 리뷰 반영)

대상: bundis 0.1.0 (RESP3-over-TCP, bun:sqlite WAL 백엔드)
환경: Apple M5 (10 cores), 24GB RAM, Bun 1.3.14, macOS (Darwin 25.5.0)
클라이언트: 순정 `Bun.RedisClient` (autoReconnect off) — wire 호환 경로 그대로 측정

## 방법론 (rev 2 변경점 포함)

- **서버는 항상 별도 프로세스** (`spawnServer`). 측정 클라이언트와 이벤트루프 공유 금지. embed는 G1 비교 전용.
- **헤드라인 구성 = 파일 + WAL** (실제 제품 구성). `:memory:`는 E1의 "스토리지 상한" 비교 레그에만.
- 케이스마다 새 서버 + 새 DB(임시 파일). 클라이언트 close → 서버 stop → DB 삭제 → `Bun.gc(true)` + 100ms.
- **닫힌 루프 순차 측정은 "service time @ concurrency 1"로 표기** — coordinated omission 때문에 부하 시 지연시간이 아님. 부하 중 꼬리지연 주장은 open-loop(의도 송신시각 기준)로만 (F3).
- **처리량 = sliding window**: depth개 워커가 완료 즉시 다음 op 발행. Promise.all 배치-드레인 버블 없음.
- 워밍업은 **측정과 동일한 워크로드**로, 최소 10k ops.
- 타이밍 루프 내 할당 금지: 키 배열 사전 생성, 샘플은 사전 할당 Float64Array.
- service time N=50k(워밍업 10k), 처리량 100k ops × 3회 반복 중앙값 + 반복별 수치 공개.
- 처리량 케이스에서 bench/server 프로세스 %CPU 샘플링(ps, 근사치) — 클라이언트 병목 플래깅용.
- 알려진 한계(명시): D1 다중 클라이언트는 단일 bench 프로세스에서 구동(16/64는 클라이언트 병목 가능, CPU로 판별), F2 구독자 동일 프로세스, max값은 일회성 이벤트 복권이라 진단용.

## 테스트 케이스

### A. 단건 service time (closed loop, concurrency 1)
| # | 케이스 |
|---|---|
| A1 | SET 64B 덮어쓰기 (100k 키스페이스) |
| A2 | GET hit |
| A3 | GET miss (메타 조회 경로 — EXISTS/TTL 동일 경로라 통합) |
| A4 | INCR |
| A6 | DEL |
| A7 | HSET(신규 필드) / HGET |
| A8 | SADD / SISMEMBER |
| A9 | EXPIRE |

### B. 파이프라인 처리량 (sliding window, depth 100, 100k ops)
| # | 케이스 |
|---|---|
| B1 | SET 덮어쓰기 (고정 키셋) |
| B1b | SET 신규 삽입 (유니크 키 — INSERT 경로) |
| B2 | GET (+CPU 샘플) |
| B3 | INCR (단일 키 경합) |
| B4 | MGET(10) |
| B5 | GET, depth 1/10/100/1000 스케일링 |

### C. 페이로드 크기 (1KB / 16KB / 256KB)
SET/GET ops/sec + MB/s + CPU. 256KB는 depth-1 라인 추가(파이프라인 단편화 영향 분리). 64B는 B군과 중복이라 제외.

### D. 동시성
| # | 케이스 |
|---|---|
| D1 | 클라이언트 1/4/16/64 (총 인플라이트 ≈128 고정, CPU 샘플, 클라이언트 병목 라벨) |
| D2 | 혼합 80/20, 50/50, 20/80 @ 16 클라이언트 |

### E. 영속성 / 키스페이스
| # | 케이스 |
|---|---|
| E1 | 파일 WAL(헤드라인) vs :memory:(상한) — SET/GET |
| E2 | 키스페이스 1k vs 100k 랜덤 GET |
| E3 | 콜드 스타트: 100k키 DB, **반복마다 새 파일 복사본**(페이지캐시 배제), 5회, spawn→ready / 첫 GET 분리 보고 |
| E4 | 지속 삽입 200k (20k 청크별 ops/sec — WAL 체크포인트 톱니 가시화, 최종 DB/WAL 크기) |

### F. 기능 경로
| # | 케이스 |
|---|---|
| F1 | 100-SET 묶음 4-way: 파이프라인 vs 순차 vs MULTI/EXEC vs MSET — MULTI 델타는 순차 레그와 비교해야 트랜잭션 오버헤드(클라이언트가 MULTI에 자동 파이프라이닝 비활성) |
| F2 | PUBLISH 팬아웃 1/10/50 구독자 (전달 msgs/sec) |
| F3 | **open-loop** GET @5k/s: 베이스라인 / TTL 분산 만료 / TTL 일괄 만료(burst) — 리퍼 스윕 꼬리지연 포착 |
| F5 | 만료 키 GET (lazy-expiry DELETE-on-read 경로, 리퍼 끔) — A3과 비교 |
| F6 | 에러 경로: WRONGTYPE GET / INCR-비숫자 vs 정상 GET 처리량 |
| F7 | APPEND 1KB×2000 단일 키 — BLOB 전체 재기록 비용 성장(첫 200 vs 끝 200 평균) |

### G. 기동 모드
| # | 케이스 |
|---|---|
| G1 | embedServer vs spawnServer — embed 수치는 "호스트 앱 영향" 라벨(동일 이벤트루프), 서버 지연시간 아님 |

### H. 컬렉션 카디널리티
| # | 케이스 |
|---|---|
| H1 | 해시/셋 1k/10k/100k: HGETALL/SMEMBERS 전체 조회(대형 응답 + 백프레셔 경로), SRANDMEMBER(1)/SPOP(1) @100k (저장층 O(n) 여부 검증), 컬렉션 통째 DEL(cascade) |

## 비측정 (명시)
- 실제 Redis 차분 비교 — 이 머신에 redis-server 없음.
- 다중 프로세스 동시 writer — 설계상 비목적(단일 writer).
- 비-loopback 네트워크.
- 전 케이스 open-loop / 10회 반복 / p99.9 — 실행 예산(~5분) 때문에 기각, F3·E3로 보완.
