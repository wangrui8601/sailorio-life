在Airflow中将复杂DAG拆分为多个Python文件，可通过模块化设计和合理分工实现代码的可维护性。以下是具体步骤和示例：

### 1. **项目结构规划**
组织目录结构，分离DAG定义、任务模块和工具函数：
```bash
my_dags/
├── __init__.py
├── dag_main.py          # 主DAG文件
├── tasks/               # 任务模块
│   ├── __init__.py
│   ├── extract.py
│   ├── transform.py
│   └── load.py
└── utils/               # 工具函数和配置
    ├── __init__.py
    └── config.py
```

### 2. **配置和工具函数**
在`utils/config.py`中定义共享配置：
```python
from airflow.models import DAG
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def get_dag_config():
    return {
        'schedule_interval': '@daily',
        'max_active_runs': 1
    }
```

### 3. **任务模块化**
将任务按功能拆分到不同模块，例如`tasks/extract.py`：
```python
from airflow.operators.python import PythonOperator

def create_extract_task(dag):
    def extract_data(**kwargs):
        print("Extracting data...")
    
    return PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag
    )
```

### 4. **主DAG文件整合**
在`dag_main.py`中导入模块并组合任务：
```python
from airflow import DAG
from datetime import datetime
from utils.config import default_args, get_dag_config
from tasks.extract import create_extract_task
from tasks.transform import create_transform_task
from tasks.load import create_load_task

# 初始化DAG
dag_config = get_dag_config()
with DAG(
    'complex_etl_dag',
    default_args=default_args,
    **dag_config
) as dag:

    # 创建任务实例
    extract_task = create_extract_task(dag)
    transform_task = create_transform_task(dag)
    load_task = create_load_task(dag)

    # 设置依赖
    extract_task >> transform_task >> load_task
```

### 5. **使用TaskGroup进一步分组（可选）**
在复杂任务流中使用TaskGroup，例如在`tasks/transform.py`中：
```python
from airflow.utils.task_group import TaskGroup

def create_transform_group(dag):
    with TaskGroup(group_id='transform_tasks') as tg:
        # 定义内部任务
        task1 = PythonOperator(task_id='transform_step1', ...)
        task2 = PythonOperator(task_id='transform_step2', ...)
        task1 >> task2
    return tg
```

### 6. **动态任务生成（可选）**
将重复任务生成逻辑提取到工具模块：
```python
# utils/generators.py
def generate_dynamic_tasks(dag, count):
    tasks = []
    for i in range(count):
        task = PythonOperator(
            task_id=f'dynamic_task_{i}',
            python_callable=lambda: print(f"Task {i}"),
            dag=dag
        )
        tasks.append(task)
    return tasks
```

### 关键点总结：
- **模块化拆分**：按功能将任务分解到独立文件，通过函数返回任务实例。
- **共享配置**：集中管理参数和配置，避免重复代码。
- **依赖管理**：在主DAG中清晰定义任务间关系。
- **TaskGroup应用**：逻辑分组复杂任务，提升UI可读性。
- **动态生成**：利用工厂函数减少重复代码。

此结构提高了代码复用性，便于团队协作和维护，同时遵循Airflow最佳实践。确保所有模块在Airflow的Python路径中可访问，并避免循环导入问题。