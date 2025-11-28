import unittest
from unittest.mock import patch, MagicMock
from your_controller_module import PolicyEngine, SafetyManager 

class TestPolicyEngine(unittest.TestCase):
    
    def setUp(self):
        self.engine = PolicyEngine()
        self.policy_config = {'type': 'cost', 'maxCostPerReplica': 5.0}
        self.scaler_config = {'minReplicas': 1, 'maxReplicas': 10}


    def test_01_scale_up_required(self):
        """Test: Load 55.0 / Target 5.0 should result in 11 (capped to max 10)"""
        current_replicas = 5
        metrics = [55.0]
        
        desired = self.engine.calculate_desired_replicas(
            current_replicas, metrics, self.policy_config, self.scaler_config
        )
        self.assertEqual(desired, 10, "Should calculate 11 and cap at 10.")

    def test_02_max_cap_applied(self):
        """Test: Very high load (80.0) must be capped by maxReplicas (10)"""
        current_replicas = 5
        metrics = [80.0]
        
        desired = self.engine.calculate_desired_replicas(
            current_replicas, metrics, self.policy_config, self.scaler_config
        )
        self.assertEqual(desired, 10, "Should cap 16 desired replicas at 10.")
        

    def test_03_scale_down_required(self):
        """Test: Low load (10.0) should trigger scale down below 11 (to 2)"""
        current_replicas = 11
        metrics = [10.0]
        
        desired = self.engine.calculate_desired_replicas(
            current_replicas, metrics, self.policy_config, self.scaler_config
        )
        self.assertEqual(desired, 2, "Should calculate a desired replica count of 2.")

    def test_04_no_change_at_equilibrium(self):
        """Test: Cost Per Replica = Max Cost (55.0 / 11 = 5.0) should result in no change"""
        current_replicas = 11
        metrics = [55.0] 
        
        desired = self.engine.calculate_desired_replicas(
            current_replicas, metrics, self.policy_config, self.scaler_config
        )
        self.assertEqual(desired, 11, "Should return current replicas when cost is at max target.")

class TestSafetyManager(unittest.TestCase):
    
    def setUp(self):
        self.manager = SafetyManager()
        self.scaler_name = "test-scaler"
        self.safety_config = {
            'maxScaleRate': 2,
            'scaleUpCooldown': '30s',
            'scaleDownCooldown': '5m'
        }

    
    def test_05_rate_limit_up(self):
        """Test: Scaling up from 5 to 10 should be limited by rate 2 (5 -> 7)"""
        current = 5
        desired = 10
        limited = self.manager.apply_rate_limits(current, desired, self.safety_config)
        self.assertEqual(limited, 7, "Scale-up should be limited to current + 2.")

    def test_06_rate_limit_down(self):
        """Test: Scaling down from 10 to 2 should be limited by rate 2 (10 -> 8)"""
        current = 10
        desired = 2
        limited = self.manager.apply_rate_limits(current, desired, self.safety_config)
        self.assertEqual(limited, 8, "Scale-down should be limited to current - 2.")

    
    @patch('time.time')
    def test_07_cooldown_active(self, mock_time):
        """Test: Cooldown active should block scaling"""
        mock_time.return_value = 100.0
        self.manager.record_scale_operation(self.scaler_name)
        

        mock_time.return_value = 120.0 
        
        can_scale = self.manager.can_scale(self.scaler_name, self.safety_config, 'up')
        self.assertFalse(can_scale, "Scaling should be blocked because 20s < 30s.")

    @patch('time.time')
    def test_08_cooldown_expired(self, mock_time):
        mock_time.return_value = 100.0 
        self.manager.record_scale_operation(self.scaler_name)
        
        mock_time.return_value = 131.0 
        
        can_scale = self.manager.can_scale(self.scaler_name, self.safety_config, 'up')
        self.assertTrue(can_scale, "Scaling should be allowed because 31s > 30s.")


if __name__ == '__main__':
    unittest.main()