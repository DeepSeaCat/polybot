from __future__ import annotations

from typing import Any, Dict, List, Optional

from .copybot_models import MirroredLot, RuntimeConfig, RuntimeStateSnapshot, to_primitive, utc_now

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover - optional dependency during bootstrap
    MongoClient = None


class BaseRepository:
    def load_runtime_state(self, default_cash: float) -> RuntimeStateSnapshot:
        raise NotImplementedError

    def save_runtime_state(self, state: RuntimeStateSnapshot) -> None:
        raise NotImplementedError

    def load_open_lots(self) -> List[MirroredLot]:
        raise NotImplementedError

    def upsert_lot(self, lot: MirroredLot) -> None:
        raise NotImplementedError

    def record_leader_events(self, events: List[Dict[str, Any]]) -> None:
        raise NotImplementedError

    def record_order(self, order: Dict[str, Any], status: str, reason: str = "") -> None:
        raise NotImplementedError

    def update_order_status(
        self,
        order_id: str,
        status: str,
        reason: str = "",
        execution_id: str = "",
    ) -> None:
        raise NotImplementedError

    def record_execution(self, execution: Dict[str, Any]) -> None:
        raise NotImplementedError

    def record_log(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        raise NotImplementedError

    def get_recent_orders(self, limit: int = 25) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_recent_executions(self, limit: int = 25) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_open_lot_documents(self) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_recent_leader_events(self, limit: int = 25) -> List[Dict[str, Any]]:
        raise NotImplementedError


class InMemoryRepository(BaseRepository):
    def __init__(self):
        self.runtime_state: Dict[str, Any] = {}
        self.lots: Dict[str, Dict[str, Any]] = {}
        self.leader_events: List[Dict[str, Any]] = []
        self.orders: List[Dict[str, Any]] = []
        self.executions: List[Dict[str, Any]] = []
        self.logs: List[Dict[str, Any]] = []

    def load_runtime_state(self, default_cash: float) -> RuntimeStateSnapshot:
        if not self.runtime_state:
            return RuntimeStateSnapshot(cash_balance_usdc=default_cash, updated_at=utc_now())
        return RuntimeStateSnapshot.from_dict(self.runtime_state, default_cash)

    def save_runtime_state(self, state: RuntimeStateSnapshot) -> None:
        self.runtime_state = to_primitive(state)

    def load_open_lots(self) -> List[MirroredLot]:
        return [MirroredLot.from_dict(item) for item in self.lots.values() if item.get("status") == "open"]

    def upsert_lot(self, lot: MirroredLot) -> None:
        self.lots[lot.lot_id] = to_primitive(lot)

    def record_leader_events(self, events: List[Dict[str, Any]]) -> None:
        self.leader_events.extend(events)

    def record_order(self, order: Dict[str, Any], status: str, reason: str = "") -> None:
        doc = dict(order)
        doc["status"] = status
        doc["reason"] = reason
        doc["updated_at"] = utc_now().isoformat()
        self.orders.append(doc)

    def update_order_status(
        self,
        order_id: str,
        status: str,
        reason: str = "",
        execution_id: str = "",
    ) -> None:
        for item in reversed(self.orders):
            if item.get("order_id") == order_id:
                item["status"] = status
                item["reason"] = reason
                item["execution_id"] = execution_id
                item["updated_at"] = utc_now().isoformat()
                break

    def record_execution(self, execution: Dict[str, Any]) -> None:
        self.executions.append(execution)

    def record_log(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.logs.append(
            {
                "level": level,
                "message": message,
                "extra": extra or {},
                "created_at": utc_now().isoformat(),
            }
        )

    def get_recent_orders(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(reversed(self.orders[-limit:]))

    def get_recent_executions(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(reversed(self.executions[-limit:]))

    def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(reversed(self.logs[-limit:]))

    def get_open_lot_documents(self) -> List[Dict[str, Any]]:
        return [dict(item) for item in self.lots.values() if item.get("status") == "open"]

    def get_recent_leader_events(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(reversed(self.leader_events[-limit:]))


class MongoRepository(BaseRepository):
    def __init__(self, uri: str, database: str):
        if MongoClient is None:
            raise RuntimeError("pymongo is not installed. Install base requirements first.")
        self.client = MongoClient(uri)
        self.db = self.client[database]

    def load_runtime_state(self, default_cash: float) -> RuntimeStateSnapshot:
        payload = self.db.runtime_state.find_one({"_id": "bot_state"}) or {}
        return RuntimeStateSnapshot.from_dict(payload, default_cash)

    def save_runtime_state(self, state: RuntimeStateSnapshot) -> None:
        payload = to_primitive(state)
        payload["_id"] = "bot_state"
        self.db.runtime_state.replace_one({"_id": "bot_state"}, payload, upsert=True)

    def load_open_lots(self) -> List[MirroredLot]:
        return [MirroredLot.from_dict(item) for item in self.db.lots.find({"status": "open"})]

    def upsert_lot(self, lot: MirroredLot) -> None:
        payload = to_primitive(lot)
        self.db.lots.replace_one({"lot_id": lot.lot_id}, payload, upsert=True)

    def record_leader_events(self, events: List[Dict[str, Any]]) -> None:
        if events:
            self.db.leader_events.insert_many(events)

    def record_order(self, order: Dict[str, Any], status: str, reason: str = "") -> None:
        payload = dict(order)
        payload["status"] = status
        payload["reason"] = reason
        payload["updated_at"] = utc_now().isoformat()
        self.db.orders.replace_one({"order_id": payload["order_id"]}, payload, upsert=True)

    def update_order_status(
        self,
        order_id: str,
        status: str,
        reason: str = "",
        execution_id: str = "",
    ) -> None:
        self.db.orders.update_one(
            {"order_id": order_id},
            {
                "$set": {
                    "status": status,
                    "reason": reason,
                    "execution_id": execution_id,
                    "updated_at": utc_now().isoformat(),
                }
            },
            upsert=True,
        )

    def record_execution(self, execution: Dict[str, Any]) -> None:
        self.db.executions.replace_one(
            {"execution_id": execution["execution_id"]},
            execution,
            upsert=True,
        )

    def record_log(self, level: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        self.db.logs.insert_one(
            {
                "level": level,
                "message": message,
                "extra": extra or {},
                "created_at": utc_now().isoformat(),
            }
        )

    def get_recent_orders(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(self.db.orders.find().sort("updated_at", -1).limit(limit))

    def get_recent_executions(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(self.db.executions.find().sort("executed_at", -1).limit(limit))

    def get_recent_logs(self, limit: int = 50) -> List[Dict[str, Any]]:
        return list(self.db.logs.find().sort("created_at", -1).limit(limit))

    def get_open_lot_documents(self) -> List[Dict[str, Any]]:
        return list(self.db.lots.find({"status": "open"}))

    def get_recent_leader_events(self, limit: int = 25) -> List[Dict[str, Any]]:
        return list(self.db.leader_events.find().sort("observed_at", -1).limit(limit))


def build_repository(config: RuntimeConfig) -> BaseRepository:
    if config.mongo.enabled:
        return MongoRepository(config.mongo.uri, config.mongo.database)
    return InMemoryRepository()
