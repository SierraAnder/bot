Execution Logs

Arbitrage opportunity found: {'Date and Time': '2025-04-14 12:05:48.099352', 'Par1': 'FDUSDUSDT', 'Par2': '1000SATSFDUSD', 'Par3': '1000SATSUSDT', 'Par1 Buy Price': Decimal('0.99760000'), 'Par2 Buy Price': Decimal('0.00003820'), 'Par3 Ask Price': Decimal('0.00003850'), 'USDT Initial Amount': Decimal('10.0'), 'USDT Final Amount': Decimal('10.0583745570000'), 'Arbitrage Ratio': Decimal('1.005837455700'), 'Profit': Decimal('0.583745570000')}
Net base amount: 10.00000000
Net alt amount: 261518.00
Final USDT amount: 10.0583745570000
📈 Real-time ratio check (WebSocket): 1.010278
⏱️ Executing arbitrage at 12:05:48.181035
💸 Requesting a loan of 10.012 FDUSD for 1000SATSFDUSD...
✅ Loan confirmed: tranId 255523028329
🔹 Executing SHORT BUY (1000SATSFDUSD)
🔹 Executing normal BUY (FDUSDUSDT)
🔹 Executing SHORT SELL (1000SATSUSDT)
❌ Error during arbitrage execution: APIError(code=-2010): Account has insufficient balance for requested action.
⚠️ Running emergency cleanup... (implement cancellations or logs if necessary)

Arbitrage opportunity found: {'Date and Time': '2025-04-14 13:20:08.899317', 'Par1': 'FDUSDUSDT', 'Par2': 'HMSTRFDUSD', 'Par3': 'HMSTRUSDT', 'Par1 Buy Price': Decimal('0.99770000'), 'Par2 Buy Price': Decimal('0.00247600'), 'Par3 Ask Price': Decimal('0.00249600'), 'USDT Initial Amount': Decimal('10.0'), 'USDT Final Amount': Decimal('10.0587951360000'), 'Ratio Arbitrage': Decimal('1.005879513600'), 'Profit': Decimal('0.587951360000')}
Net base amount: 10.00000000
Net alt amount: 4034.00
Final USDT amount: 10.0587951360000
📈 Real-time ratio check (WebSocket): 1.010401
⏱️ Running arbitrage at 13:20:08.983343
⏱️ Running loan request 13:20:08.983376
💸 Requesting a loan of 10.012 FDUSD for HMSTRFDUSD...
⏱️ Checking loan 13:20:09.220661
✅ Loan confirmed: tranId 255533464209
🔹 Executing SHORT BUY (HMSTRFDUSD)
🔹 Executing normal BUY (FDUSDUSDT)
🔹 Executing SHORT SELL (HMSTRUSDT)
❌ Error during arbitrage execution: APIError(code=-2010): Account has insufficient balance for requested action.
