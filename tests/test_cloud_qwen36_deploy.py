import json
import unittest


class PAIQwen36DeployTests(unittest.TestCase):
    def test_build_config_mounts_oss_and_starts_vllm_with_fp8_model(self):
        from scripts.deploy_pai_qwen36_fp8 import build_config

        config = build_config(
            {
                "PAI_MODEL_SOURCE": "custom",
                "PAI_SERVICE_NAME": "qwen36-fp8",
                "PAI_MODEL_PATH": "oss://models/qwen36-fp8/",
                "PAI_INSTANCE_TYPE": "ecs.gn7i.8xlarge",
                "PAI_GPU_COUNT": "2",
                "PAI_TP": "2",
                "PAI_WORKSPACE_ID": "ws-123",
                "PAI_VLLM_IMAGE": "registry.example/vllm:validated",
                "PAI_MAX_MODEL_LEN": "16384",
                "PAI_SERVED_MODEL_NAME": "qwen3.6-27b-fp8",
            }
        )

        self.assertEqual(config["metadata"]["workspace_id"], "ws-123")
        self.assertEqual(config["metadata"]["gpu"], 2)
        self.assertEqual(config["cloud"]["computing"]["instances"][0]["type"], "ecs.gn7i.8xlarge")
        self.assertEqual(config["storage"][0]["oss"]["path"], "oss://models/qwen36-fp8/")
        self.assertEqual(config["storage"][0]["mount_path"], "/mnt/model")
        container = config["containers"][0]
        self.assertEqual(container["image"], "registry.example/vllm:validated")
        self.assertEqual(container["port"], 8000)
        self.assertIn("vllm serve /mnt/model", container["script"])
        self.assertIn("--tensor-parallel-size 2", container["script"])
        self.assertIn("--max-model-len 16384", container["script"])

    def test_build_config_rejects_missing_storage_or_instance_type(self):
        from scripts.deploy_pai_qwen36_fp8 import build_config

        with self.assertRaisesRegex(ValueError, "PAI_MODEL_PATH"):
            build_config({"PAI_MODEL_SOURCE": "custom", "PAI_INSTANCE_TYPE": "ecs.gn7i.8xlarge"})
        with self.assertRaisesRegex(ValueError, "PAI_INSTANCE_TYPE"):
            build_config({"PAI_MODEL_SOURCE": "custom", "PAI_MODEL_PATH": "oss://models/qwen36-fp8/"})

    def test_pai_config_does_not_include_project_llm_key(self):
        from scripts.deploy_pai_qwen36_fp8 import build_config

        config = build_config(
            {
                "PAI_MODEL_SOURCE": "custom",
                "PAI_MODEL_PATH": "oss://models/qwen36-fp8/",
                "PAI_INSTANCE_TYPE": "ecs.gn7i.8xlarge",
                "LLM_API_KEY": "must-not-leak",
            }
        )
        self.assertNotIn("must-not-leak", json.dumps(config))

    def test_builtin_plan_does_not_require_or_mount_oss(self):
        from scripts.deploy_pai_qwen36_fp8 import build_deployment

        plan = build_deployment(
            {
                "PAI_MODEL_SOURCE": "builtin",
                "PAI_MODEL_NAME": "Qwen3.6-27B-FP8",
                "PAI_SERVICE_NAME": "qwen36-native",
                "PAI_INSTANCE_TYPE": "ecs.gn7i.20xlarge",
            }
        )

        self.assertEqual(plan["deployment_mode"], "builtin")
        self.assertEqual(plan["model"]["name"], "Qwen3.6-27B-FP8")
        self.assertNotIn("storage", plan)
        self.assertNotIn("oss://", json.dumps(plan))


class TioneQwen36DeployTests(unittest.TestCase):
    def test_build_request_mounts_cfs_and_uses_platform_auth(self):
        from scripts.deploy_tione_qwen36_fp8 import build_request

        request = build_request(
            {
                "TIONE_MODEL_SOURCE": "custom",
                "TIONE_SERVICE_GROUP_NAME": "qwen36-fp8",
                "TIONE_REGION": "ap-guangzhou",
                "TIONE_INSTANCE_TYPE": "TI.GN7.20XLARGE320.POST",
                "TIONE_CFS_ID": "cfs-123",
                "TIONE_CFS_PATH": "/models/qwen36-fp8",
                "TIONE_IMAGE_URL": "mirror.ccs.tencentyun.com/vllm/vllm-openai:v0.11.0",
                "TIONE_IMAGE_TYPE": "CUSTOM",
                "TIONE_TP": "4",
                "TIONE_SERVED_MODEL_NAME": "qwen3.6-27b-fp8",
            }
        )

        self.assertEqual(request["AuthorizationEnable"], True)
        self.assertEqual(request["ServicePort"], 8000)
        self.assertEqual(request["ImageInfo"]["ImageType"], "CUSTOM")
        self.assertEqual(request["ImageInfo"]["ImageUrl"], "mirror.ccs.tencentyun.com/vllm/vllm-openai:v0.11.0")
        self.assertEqual(request["VolumeMounts"][0]["CFSConfig"]["Id"], "cfs-123")
        self.assertEqual(request["VolumeMounts"][0]["CFSConfig"]["Path"], "/models/qwen36-fp8")
        self.assertEqual(request["VolumeMounts"][0]["MountPath"], "/data/model")
        self.assertIn("--served-model-name qwen3.6-27b-fp8", request["Command"])
        self.assertIn("--tensor-parallel-size 4", request["Command"])

    def test_dist_request_contains_ray_bootstrap_and_symm_mem_workaround(self):
        from scripts.deploy_tione_qwen36_fp8 import build_request

        request = build_request(
            {
                "TIONE_MODEL_SOURCE": "custom",
                "TIONE_SERVICE_GROUP_NAME": "qwen36-fp8-dist",
                "TIONE_INSTANCE_TYPE": "TI.GN7.20XLARGE320.POST",
                "TIONE_CFS_ID": "cfs-123",
                "TIONE_CFS_PATH": "/models/qwen36-fp8",
                "TIONE_DEPLOY_TYPE": "DIST",
                "TIONE_INSTANCE_PER_REPLICA": "2",
            }
        )

        self.assertEqual(request["DeployType"], "DIST")
        self.assertEqual(request["InstancePerReplicas"], 2)
        self.assertIn('if [ "$RANK" = "0" ]', request["Command"])
        self.assertIn("ray start --head", request["Command"])
        self.assertIn("VLLM_ALLREDUCE_USE_SYMM_MEM", request["Env"] and json.dumps(request["Env"]))

    def test_tione_requires_cfs_and_instance_type(self):
        from scripts.deploy_tione_qwen36_fp8 import build_request

        with self.assertRaisesRegex(ValueError, "TIONE_CFS_ID"):
            build_request(
                {
                    "TIONE_MODEL_SOURCE": "custom",
                    "TIONE_SERVICE_GROUP_NAME": "qwen36-fp8",
                    "TIONE_INSTANCE_TYPE": "TI.GN7.20XLARGE320.POST",
                    "TIONE_CFS_PATH": "/models/qwen36-fp8",
                }
            )
        with self.assertRaisesRegex(ValueError, "TIONE_INSTANCE_TYPE"):
            build_request(
                {
                    "TIONE_MODEL_SOURCE": "custom",
                    "TIONE_SERVICE_GROUP_NAME": "qwen36-fp8",
                    "TIONE_CFS_ID": "cfs-123",
                    "TIONE_CFS_PATH": "/models/qwen36-fp8",
                }
            )

    def test_builtin_plan_does_not_require_or_mount_cfs(self):
        from scripts.deploy_tione_qwen36_fp8 import build_deployment

        plan = build_deployment(
            {
                "TIONE_MODEL_SOURCE": "builtin",
                "TIONE_SERVICE_GROUP_NAME": "qwen36-native",
                "TIONE_BUILTIN_MODEL_ID": "qwen3.6-27b",
                "TIONE_BUILTIN_MODEL_NAME": "Qwen3.6-27B",
                "TIONE_INSTANCE_TYPE": "TI.GN7.20XLARGE320.POST",
            }
        )

        self.assertEqual(plan["deployment_mode"], "builtin")
        self.assertEqual(plan["model"]["id"], "qwen3.6-27b")
        self.assertNotIn("VolumeMounts", plan)
        self.assertNotIn("cfs-", json.dumps(plan))


if __name__ == "__main__":
    unittest.main()
