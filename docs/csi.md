[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/perigeos-test.txt
/data/perigeos-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- cat /data/perigeos-test.txt
Hello from Perigeos Pawn!
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- cp /data/perigeos-test.txt /data/perigeos-test2.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/perigeos-test.txt
/data/perigeos-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/perigeos-test.txt
/data/perigeos-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- cp /data/perigeos-test.txt /data/perigeos-test2.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/perigeos-test.txt
/data/perigeos-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- touch /data/touch-test.txt
kubectl exec seaweed-pvc-test -- ls /data/touch-test.txt
/data/touch-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/
perigeos-test.txt
perigeos-test2.txt
touch-test.txt
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- cat /data/perigeos-test2.txt
Hello from Perigeos Pawn!
[engi@engix99 periapsis]$ kubectl delete pod seaweed-pvc-test 
pod "seaweed-pvc-test" deleted from default namespace
[engi@engix99 periapsis]$ kubectl apply -f deploy/seaweedfs/test-seaweed-pvc.yaml 
pod/seaweed-pvc-test created
[engi@engix99 periapsis]$ kubectl exec seaweed-pvc-test -- ls /data/
perigeos-test-new.txt
perigeos-test.txt
perigeos-test2.txt
touch-test.txt
