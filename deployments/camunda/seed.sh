#!/bin/sh
set -e

ZEEBE_REST="http://zeebe:8080"

echo "=== Camunda Test Data Seeder ==="
echo ""

echo "Deploying test-process.bpmn..."
curl -sf -X POST "$ZEEBE_REST/v2/deployments" \
  -F "resources=@/camunda/test-process.bpmn" \
  -o /dev/null
echo "Process deployed."
echo ""

echo "Creating 10 process instances..."
for i in $(seq 1 10); do
  curl -sf -X POST "$ZEEBE_REST/v2/process-instances" \
    -H "Content-Type: application/json" \
    -d "{\"processDefinitionId\":\"test-backup-process\",\"variables\":{\"orderId\":\"order-$i\",\"amount\":$((i * 100))}}" \
    -o /dev/null
  echo "  Created instance $i (orderId=order-$i, amount=$((i * 100)))"
done

echo ""
echo "=== Seeding complete ==="
echo "  10 active process instances waiting at 'Process Order' service task"
echo "  View in Operate: http://localhost:8081"
echo "  Backup Controller: http://localhost:8080"
