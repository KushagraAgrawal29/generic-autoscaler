import asyncio
import logging
import time
from typing import Dict, List, Any
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class MetricPlugin:
    
    def get_metric(self, config: Dict) -> float:
        raise NotImplementedError("Plugin must implement get_metric")

class PrometheusPlugin(MetricPlugin):
    """⭐ CONCRETE PLUGIN: Fetches metrics from Prometheus"""
    
    def get_metric(self, config: Dict) -> float:
        query = config.get('query', '')
        logger.debug(f"📊 Prometheus query: {query}")
        
        if 'cpu' in query:
            return 75.0 
        elif 'memory' in query:
            return 65.0 
        elif 'http_request' in query:
            return 150.0 
        else:
            return 50.0 

class RedisPlugin(MetricPlugin):
    
    def get_metric(self, config: Dict) -> float:
        host = config.get('host', 'redis-service')
        queue_name = config.get('queue_name', 'default')
        logger.debug(f"📊 Redis check: {queue_name} on {host}")
        
        return 10.0 


class PolicyEngine:
    
    def calculate_desired_replicas(
        self, 
        current_replicas: int,
        metrics: List[float],
        policy_config: Dict,
        scaler_config: Dict
    ) -> int:
        
        policy_type = policy_config.get('type', 'slo')
        
        if policy_type == 'slo':
            return self._slo_based_scaling(current_replicas, metrics, policy_config, scaler_config)
        elif policy_type == 'cost':
            return self._cost_aware_scaling(current_replicas, metrics, policy_config, scaler_config)
        else:
            logger.warning(f"Unknown policy type: {policy_type}")
            return current_replicas
    
    def _slo_based_scaling(self, current_replicas: int, metrics: List[float], 
                          policy_config: Dict, scaler_config: Dict) -> int:
        
        target = policy_config.get('sloTarget', 80.0)
        current_metric = sum(metrics) / len(metrics) if metrics else 0
        
        logger.info(f"🎯 SLO Policy: current={current_metric:.2f}, target={target}")
        
        ratio = current_metric / target if target > 0 else 1
        desired = int(current_replicas * ratio) 
        
        min_replicas = scaler_config.get('minReplicas', 1)
        max_replicas = scaler_config.get('maxReplicas', 10)
        
        desired = max(min_replicas, min(max_replicas, desired))
        
        return desired
    
    def _cost_aware_scaling(self, current_replicas: int, metrics: List[float],
                           policy_config: Dict, scaler_config: Dict) -> int:
        
        max_cost = policy_config.get('maxCostPerReplica', 5.0) 
        current_metric = sum(metrics) / len(metrics) if metrics else 0 
        cost_per_replica = current_metric / current_replicas if current_replicas > 0 else 0
        
        max_replicas = scaler_config.get('maxReplicas', 10)
        
        if cost_per_replica > max_cost:
            desired_by_ratio = int(current_metric / max_cost) 
            if (current_metric / max_cost) > desired_by_ratio
                desired_by_ratio += 1

            final_desired = min(desired_by_ratio, max_replicas) 
            
            logger.info(f"❌ Policy Engine Scale UP: Calculated={desired_by_ratio}, Capped={final_desired}")
            
            return final_desired
            
        elif cost_per_replica < max_cost * 0.5:
            desired_by_ratio = int(current_metric / (max_cost * 0.8))
            min_replicas = scaler_config.get('minReplicas', 1)

            return max(desired_by_ratio, min_replicas)
            
        else:
            return current_replicas


class SafetyManager:
    
    def __init__(self):
        self.last_scale_operations: Dict[str, float] = {} 
    
    def can_scale(self, scaler_name: str, safety_config: Dict, direction: str) -> bool:
        """
        🚦 Check if scaling is allowed based on safety rules
        """
        cooldown_key = f'scale{direction.capitalize()}Cooldown'
        cooldown_seconds = self._parse_duration(safety_config.get(cooldown_key, '5m'))
        
        last_scale = self.last_scale_operations.get(scaler_name, 0)
        time_since_last = time.time() - last_scale

        logger.debug(f"⏳ Cooldown check: Since={time_since_last:.1f}s, Required={cooldown_seconds}s") # ADD THIS LINE
        
        if time_since_last < cooldown_seconds:
            logger.info(f"⏳ Cooldown active for {direction}: {time_since_last:.1f}s < {cooldown_seconds}s")
            return False
        
        return True
    
    def apply_rate_limits(self, current: int, desired: int, safety_config: Dict) -> int:
        """
        🚧 Apply rate limiting to prevent drastic changes
        """
        max_rate = int(safety_config.get('maxScaleRate', 2))
        
        if desired > current:
            # Scale up
            limited = min(current + max_rate, desired)
            if limited != desired:
                logger.info(f"📏 Rate limited scale-up: {desired} -> {limited}")
            return limited
        elif desired < current:
            # Scale down 
            limited = max(current - max_rate, desired)
            if limited != desired:
                logger.info(f"📏 Rate limited scale-down: {desired} -> {limited}")
            return limited
        
        return desired
    
    def record_scale_operation(self, scaler_name: str):
        """📝 Record scaling operation for cooldown tracking"""
        self.last_scale_operations[scaler_name] = time.time()
        logger.info(f"📝 Cooldown Set: {scaler_name} recorded at {time.time()}") # ADD THIS LINE
    
    def _parse_duration(self, duration_str: str) -> int:
        """Convert duration string like '5m' to seconds"""
        units = {'s': 1, 'm': 60, 'h': 3600, 'd': 86400}
        unit = duration_str[-1]
        try:
            value = int(duration_str[:-1])
            return value * units.get(unit, 1)
        except ValueError:
            logger.error(f"Invalid duration string: {duration_str}")
            return 300


class GeneralScalerController:
    """
    🎛️ MAIN CONTROLLER: The brain that coordinates everything
    """
    
    def __init__(self):
        try:
            config.load_incluster_config() 
            logger.info("✅ Loaded in-cluster kubeconfig")
        except config.ConfigException:
            config.load_kube_config() 
            logger.info("✅ Loaded local kubeconfig")
        
        self.apps_v1 = client.AppsV1Api()
        self.custom_api = client.CustomObjectsApi()
        
        self.metric_plugins = {
            'prometheus': PrometheusPlugin(),
            'redis': RedisPlugin(),
        }
        
        self.policy_engine = PolicyEngine()
        self.safety_manager = SafetyManager()
        
        logger.info("🚀 GeneralScaler Controller initialized")
    
    async def run(self):
        """🔄 Main control loop - runs forever"""
        logger.info("Starting controller main loop...")
        
        while True:
            try:
                scalers = await asyncio.to_thread(self._get_all_scalers)
                logger.info(f"🔍 Found {len(scalers)} GeneralScaler(s) to reconcile")
                
                reconciliation_tasks = [self._reconcile_scaler(scaler) for scaler in scalers]
                await asyncio.gather(*reconciliation_tasks)
                
                # 3. Wait before next iteration
                await asyncio.sleep(30)
                
            except Exception as e:
                logger.error(f"💥 Error in controller loop: {e}")
                await asyncio.sleep(10)
    
    def _get_all_scalers(self) -> List[Dict]:
        """📋 Get all GeneralScaler custom resources from all namespaces (Synchronous)"""
        try:
            response = self.custom_api.list_cluster_custom_object(
                group="autoscaling.example.com",
                version="v1alpha1",
                plural="generalscalers"
            )
            return response.get('items', [])
        except ApiException as e:
            logger.error(f"❌ Failed to list GeneralScalers: {e.status} - {e.reason}")
            return []
    
    async def _reconcile_scaler(self, scaler: Dict):
        """
        🔄 Reconciliation loop for a single GeneralScaler
        """
        scaler_name = scaler['metadata']['name']
        namespace = scaler['metadata'].get('namespace', 'default')
        spec = scaler.get('spec', {})
        
        logger.info(f"🔄 Reconciling GeneralScaler: {namespace}/{scaler_name}")
        
        try:
            target_ref = spec.get('targetRef', {})
            target_name = target_ref.get('name')
            
            if not target_name:
                logger.error(f"❌ No targetRef.name in {scaler_name}")
                return
            
            deployment = await asyncio.to_thread(
                self.apps_v1.read_namespaced_deployment,
                name=target_name,
                namespace=namespace
            )
            
            current_replicas = deployment.spec.replicas or 1
            logger.info(f"🎯 Target: {target_name}, current replicas: {current_replicas}")
            
            metrics = self._collect_metrics(spec.get('metrics', []), scaler_name)
            
            if not metrics:
                logger.warning(f"⚠️ No metrics collected for {scaler_name}")
                return
            
            desired_replicas = self.policy_engine.calculate_desired_replicas(
                current_replicas=current_replicas,
                metrics=metrics,
                policy_config=spec.get('policy', {}),
                scaler_config=spec
            )
            
            logger.info(f"📊 Metrics: {metrics}, Policy Desired: {desired_replicas}")
            
            
            if desired_replicas == current_replicas:
                logger.info("✅ Policy calculated no change needed.")
                await self._update_scaler_status(scaler_name, namespace, current_replicas, deployment.metadata.uid)
                return
            
            direction = 'up' if desired_replicas > current_replicas else 'down'
            safety_config = spec.get('safety', {})
            
            # Check cooldown
            if not self.safety_manager.can_scale(scaler_name, safety_config, direction):
                logger.info(f"⏸️ Scaling {direction} skipped due to cooldown")
                await self._update_scaler_status(scaler_name, namespace, current_replicas, deployment.metadata.uid, reason="CooldownActive")
                return
            
            # Apply rate limiting
            limited_replicas = self.safety_manager.apply_rate_limits(
                current_replicas, desired_replicas, safety_config
            )
            
            if limited_replicas != current_replicas:
                await self._scale_deployment(
                    deployment=deployment,
                    desired_replicas=limited_replicas,
                    scaler_name=scaler_name,
                    namespace=namespace
                )
                self.safety_manager.record_scale_operation(scaler_name)
            
            await self._update_scaler_status(scaler_name, namespace, limited_replicas, deployment.metadata.uid)
                
        except ApiException as e:
            if e.status == 404:
                logger.error(f"❌ Target deployment '{target_name}' not found for {scaler_name}")
            else:
                logger.error(f"❌ Kubernetes API error for {scaler_name}: {e.status} - {e.reason}")
        except Exception as e:
            logger.error(f"💥 Unexpected error reconciling {scaler_name}: {e}")
    
    def _collect_metrics(self, metrics_config: List[Dict], scaler_name: str) -> List[float]:
        """📈 Collect metrics from all configured plugin sources"""
        collected_metrics = []
        for metric_config in metrics_config:
            plugin_name = metric_config.get('plugin')
            
            if plugin_name in self.metric_plugins:
                try:
                    plugin = self.metric_plugins[plugin_name]
                    metric_value = plugin.get_metric(metric_config.get('config', {}))
                    collected_metrics.append(metric_value)
                except Exception as e:
                    logger.error(f"❌ Metric plugin {plugin_name} failed for {scaler_name}: {e}")
            else:
                logger.warning(f"⚠️ Unknown metric plugin: {plugin_name}")
        return collected_metrics
    
    async def _scale_deployment(self, deployment: client.V1Deployment, desired_replicas: int, 
                              scaler_name: str, namespace: str):
        """⚡ Scale the target deployment to desired replicas (Synchronous, run in thread)"""
        
        def scale_sync():
            # Create patch body
            patch = {'spec': {'replicas': desired_replicas}}
            
            # Apply patch to deployment
            self.apps_v1.patch_namespaced_deployment(
                name=deployment.metadata.name,
                namespace=namespace,
                body=patch
            )
            logger.info(f"🎯 Scaled {deployment.metadata.name} from {deployment.spec.replicas} to {desired_replicas} replicas")
            
        await asyncio.to_thread(scale_sync)
    
    async def _update_scaler_status(self, scaler_name: str, namespace: str, 
                                  current_replicas: int, deployment_uid: str, reason: str = "ScalingApplied"):
        """📝 Update GeneralScaler status subresource (Synchronous, run in thread)"""
        
        def update_sync():
            
        
            now_iso = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            
            status_patch = {
                'status': {
                    'currentReplicas': current_replicas,
                    'lastScaleTime': now_iso,
                    'conditions': [
                        {
                            'type': 'Ready',
                            'status': 'True',
                            'lastTransitionTime': now_iso,
                            'reason': reason,
                            'message': f"Target {deployment_uid} currently at {current_replicas} replicas."
                        }
                    ]
                }
            }
            
            # CRD must have subresources: status: {} enabled for this to work
            self.custom_api.patch_namespaced_custom_object_status(
                group="autoscaling.example.com",
                version="v1alpha1",
                plural="generalscalers",
                name=scaler_name,
                namespace=namespace,
                body=status_patch
            )
            
        try:
            await asyncio.to_thread(update_sync)
        except Exception as e:
            logger.warning(f"⚠️ Could not update status for {scaler_name}: {e}")

async def main():
    """🎬 Entry point - start the controller"""
    controller = GeneralScalerController()
    await controller.run()

if __name__ == "__main__":
    asyncio.run(main())