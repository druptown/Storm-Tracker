"""Exact bounded global assignment with births, deaths and alternatives.

The solver receives externally calibrated scalar costs.  It performs no speed
gate and requires a cost for every previous/current pair, so an extreme
candidate remains in the immutable problem even when another hypothesis wins.
The dynamic program is exact within an explicit detection-count capacity.
"""
from __future__ import annotations

import math
import heapq
from dataclasses import dataclass
from typing import Iterable


GLOBAL_ASSIGNMENT_VERSION = "stv3-exact-bounded-global-assignment-v3"


@dataclass(frozen=True, slots=True)
class AssignmentEdgeCost:
    previous_detection_id: str
    current_detection_id: str
    cost: float
    cost_model_id: str
    components: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        if (
            not isinstance(self.previous_detection_id, str)
            or not self.previous_detection_id
            or not isinstance(self.current_detection_id, str)
            or not self.current_detection_id
        ):
            raise ValueError("AssignmentEdgeCost vereist twee detectie-ID's")
        if not math.isfinite(self.cost):
            raise ValueError("assignment edge cost moet eindig zijn")
        if not isinstance(self.cost_model_id, str) or not self.cost_model_id:
            raise ValueError("AssignmentEdgeCost vereist een cost_model_id")
        if not isinstance(self.components, tuple):
            raise TypeError("costcomponenten moeten immutable tuples zijn")
        names = [name for name, _value in self.components]
        if (
            len(names) != len(set(names))
            or any(not name or not isinstance(name, str) for name in names)
            or any(not math.isfinite(value) for _name, value in self.components)
        ):
            raise ValueError("costcomponenten moeten uniek, benoemd en eindig zijn")
        if self.components and not math.isclose(
            sum(value for _name, value in self.components),
            self.cost,
            rel_tol=1e-12,
            abs_tol=1e-9,
        ):
            raise ValueError("som van costcomponenten verschilt van edge cost")


@dataclass(frozen=True, slots=True)
class GlobalAssignmentProblem:
    previous_detection_ids: tuple[str, ...]
    current_detection_ids: tuple[str, ...]
    edge_costs: tuple[AssignmentEdgeCost, ...]
    death_costs: tuple[tuple[str, float], ...]
    birth_costs: tuple[tuple[str, float], ...]
    policy_id: str

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, tuple)
            for value in (
                self.previous_detection_ids,
                self.current_detection_ids,
                self.edge_costs,
                self.death_costs,
                self.birth_costs,
            )
        ):
            raise TypeError("assignmentproblemvelden moeten immutable tuples zijn")
        _validate_identifiers(self.previous_detection_ids, "previous")
        _validate_identifiers(self.current_detection_ids, "current")
        if not isinstance(self.policy_id, str) or not self.policy_id:
            raise ValueError("GlobalAssignmentProblem vereist policy_id")
        if any(not isinstance(item, AssignmentEdgeCost) for item in self.edge_costs):
            raise TypeError("edge_costs vereist AssignmentEdgeCost-objecten")
        pairs = [
            (item.previous_detection_id, item.current_detection_id)
            for item in self.edge_costs
        ]
        if len(pairs) != len(set(pairs)):
            raise ValueError("dubbele assignment edge")
        expected = {
            (old_id, new_id)
            for old_id in self.previous_detection_ids
            for new_id in self.current_detection_ids
        }
        if set(pairs) != expected:
            raise ValueError(
                "edge_costs moeten de volledige vorige×huidige kandidatenset bevatten"
            )
        cost_model_ids = {item.cost_model_id for item in self.edge_costs}
        if len(cost_model_ids) > 1:
            raise ValueError("alle edge costs moeten hetzelfde costmodel gebruiken")
        _validate_event_costs(
            self.death_costs,
            self.previous_detection_ids,
            "death",
        )
        _validate_event_costs(
            self.birth_costs,
            self.current_detection_ids,
            "birth",
        )


@dataclass(frozen=True, slots=True)
class AssignmentSolution:
    rank: int
    total_cost: float
    matches: tuple[tuple[str, str], ...]
    deaths: tuple[str, ...]
    births: tuple[str, ...]

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, tuple)
            for value in (self.matches, self.deaths, self.births)
        ):
            raise TypeError("AssignmentSolution-beslissingen moeten tuples zijn")
        if (
            isinstance(self.rank, bool)
            or not isinstance(self.rank, int)
            or self.rank <= 0
        ):
            raise ValueError("AssignmentSolution rank moet positief zijn")
        if not math.isfinite(self.total_cost):
            raise ValueError("AssignmentSolution total_cost moet eindig zijn")
        previous = [old_id for old_id, _new_id in self.matches]
        current = [new_id for _old_id, new_id in self.matches]
        if (
            len(previous) != len(set(previous))
            or len(current) != len(set(current))
            or any(
                not isinstance(item, str) or not item
                for item in previous + current
            )
        ):
            raise ValueError("AssignmentSolution matches moeten 1:1 en geldig zijn")
        if (
            len(self.deaths) != len(set(self.deaths))
            or len(self.births) != len(set(self.births))
            or any(
                not isinstance(item, str) or not item
                for item in self.deaths + self.births
            )
        ):
            raise ValueError("AssignmentSolution birth/death-ID's moeten uniek zijn")
        if set(previous) & set(self.deaths) or set(current) & set(self.births):
            raise ValueError("AssignmentSolution bevat tegenstrijdige beslissingen")

    @property
    def decision_count(self) -> int:
        return len(self.matches) + len(self.deaths) + len(self.births)


@dataclass(frozen=True, slots=True)
class GlobalAssignmentResult:
    problem: GlobalAssignmentProblem
    solutions: tuple[AssignmentSolution, ...]
    exact: bool
    maximum_current_detections: int | None
    solver: str
    explored_subproblems: int = 0
    maximum_subproblems: int | None = None
    search_exhausted: bool = False
    capacity_exhausted: bool = False
    algorithm_version: str = GLOBAL_ASSIGNMENT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.problem, GlobalAssignmentProblem):
            raise TypeError("assignment result vereist het bronprobleem")
        if (
            not isinstance(self.solutions, tuple)
            or not self.solutions
            or any(not isinstance(item, AssignmentSolution) for item in self.solutions)
        ):
            raise ValueError("assignment result vereist minstens een oplossing")
        if tuple(item.rank for item in self.solutions) != tuple(
            range(1, len(self.solutions) + 1)
        ):
            raise ValueError("assignment solution ranks moeten aaneensluitend zijn")
        ordering = tuple(
            (item.total_cost, item.matches, item.deaths, item.births)
            for item in self.solutions
        )
        if ordering != tuple(sorted(ordering)):
            raise ValueError("assignment solutions zijn niet canoniek gerangschikt")
        if type(self.exact) is not bool:
            raise TypeError("assignment exact moet bool zijn")
        if (
            self.maximum_current_detections is not None
            and (
                isinstance(self.maximum_current_detections, bool)
                or not isinstance(self.maximum_current_detections, int)
                or self.maximum_current_detections < 0
            )
        ):
            raise ValueError("maximum_current_detections is ongeldig")
        if not isinstance(self.solver, str) or not self.solver:
            raise ValueError("assignment solver-ID ontbreekt")
        if (
            isinstance(self.explored_subproblems, bool)
            or not isinstance(self.explored_subproblems, int)
            or self.explored_subproblems < 0
        ):
            raise ValueError("explored_subproblems is ongeldig")
        if self.maximum_subproblems is not None and (
            isinstance(self.maximum_subproblems, bool)
            or not isinstance(self.maximum_subproblems, int)
            or self.maximum_subproblems <= 0
        ):
            raise ValueError("maximum_subproblems is ongeldig")
        if (
            self.maximum_subproblems is not None
            and self.explored_subproblems > self.maximum_subproblems
        ):
            raise ValueError("assignment overschreed subproblem capacity")
        if type(self.search_exhausted) is not bool or type(self.capacity_exhausted) is not bool:
            raise TypeError("assignment zoekstatus moet bool zijn")
        if self.search_exhausted and self.capacity_exhausted:
            raise ValueError("zoekruimte kan niet tegelijk uitgeput en gecapt zijn")
        if self.algorithm_version != GLOBAL_ASSIGNMENT_VERSION:
            raise ValueError("onbekende global-assignmentversie")

    @property
    def best(self) -> AssignmentSolution:
        return self.solutions[0]

    @property
    def alternatives(self) -> tuple[AssignmentSolution, ...]:
        return self.solutions[1:]


@dataclass(frozen=True, slots=True)
class _PartialSolution:
    cost: float
    matches: tuple[tuple[str, str], ...]
    deaths: tuple[str, ...]


def solve_exact_global_assignment(
    problem: GlobalAssignmentProblem,
    *,
    solution_limit: int,
    maximum_current_detections: int,
) -> GlobalAssignmentResult:
    """Return the exact top-k decisions under an explicit capacity boundary."""

    if not isinstance(problem, GlobalAssignmentProblem):
        raise TypeError("solver vereist een GlobalAssignmentProblem")
    if (
        isinstance(solution_limit, bool)
        or not isinstance(solution_limit, int)
        or solution_limit <= 0
    ):
        raise ValueError("solution_limit moet een positief geheel getal zijn")
    if (
        isinstance(maximum_current_detections, bool)
        or not isinstance(maximum_current_detections, int)
        or maximum_current_detections < 0
    ):
        raise ValueError(
            "maximum_current_detections moet een niet-negatief geheel getal zijn"
        )
    if len(problem.current_detection_ids) > maximum_current_detections:
        raise ValueError(
            "exacte assignmentcapaciteit overschreden; geen approximatie uitgevoerd"
        )

    previous = tuple(sorted(problem.previous_detection_ids))
    current = tuple(sorted(problem.current_detection_ids))
    edge_cost = {
        (item.previous_detection_id, item.current_detection_id): item.cost
        for item in problem.edge_costs
    }
    death_cost = dict(problem.death_costs)
    birth_cost = dict(problem.birth_costs)

    states: dict[int, tuple[_PartialSolution, ...]] = {
        0: (_PartialSolution(cost=0.0, matches=(), deaths=()),)
    }
    for previous_id in previous:
        next_states: dict[int, list[_PartialSolution]] = {}
        for used_mask, partials in states.items():
            for partial in partials:
                _retain_partial(
                    next_states,
                    used_mask,
                    _PartialSolution(
                        cost=partial.cost + death_cost[previous_id],
                        matches=partial.matches,
                        deaths=partial.deaths + (previous_id,),
                    ),
                    solution_limit,
                )
                for current_index, current_id in enumerate(current):
                    bit = 1 << current_index
                    if used_mask & bit:
                        continue
                    _retain_partial(
                        next_states,
                        used_mask | bit,
                        _PartialSolution(
                            cost=(
                                partial.cost
                                + edge_cost[(previous_id, current_id)]
                            ),
                            matches=partial.matches
                            + ((previous_id, current_id),),
                            deaths=partial.deaths,
                        ),
                        solution_limit,
                    )
        states = {
            mask: tuple(values) for mask, values in next_states.items()
        }

    completed: list[
        tuple[
            float,
            tuple[tuple[str, str], ...],
            tuple[str, ...],
            tuple[str, ...],
        ]
    ] = []
    for used_mask, partials in states.items():
        births = tuple(
            current_id
            for index, current_id in enumerate(current)
            if not used_mask & (1 << index)
        )
        added_birth_cost = sum(birth_cost[item] for item in births)
        for partial in partials:
            completed.append(
                (
                    partial.cost + added_birth_cost,
                    partial.matches,
                    partial.deaths,
                    births,
                )
            )
    completed.sort(key=lambda item: (item[0], item[1], item[2], item[3]))
    unique: list[
        tuple[
            float,
            tuple[tuple[str, str], ...],
            tuple[str, ...],
            tuple[str, ...],
        ]
    ] = []
    decisions_seen: set[
        tuple[
            tuple[tuple[str, str], ...],
            tuple[str, ...],
            tuple[str, ...],
        ]
    ] = set()
    for item in completed:
        decision = item[1:]
        if decision in decisions_seen:
            continue
        decisions_seen.add(decision)
        unique.append(item)
        if len(unique) >= solution_limit:
            break
    if not unique:
        raise AssertionError("globale assignment leverde geen oplossing")
    return GlobalAssignmentResult(
        problem=problem,
        solutions=tuple(
            AssignmentSolution(
                rank=rank,
                total_cost=item[0],
                matches=item[1],
                deaths=item[2],
                births=item[3],
            )
            for rank, item in enumerate(unique, start=1)
        ),
        exact=True,
        maximum_current_detections=maximum_current_detections,
        solver="bounded_exact_top_k_dynamic_program",
    )


def solve_global_assignment_one_best(
    problem: GlobalAssignmentProblem,
) -> GlobalAssignmentResult:
    """Solve an arbitrary-size one-best problem by exact Hungarian assignment.

    Unlike the top-k audit solver, this method has no exponential
    detection-count boundary.  The sparse augmented graph contains every
    previous/current edge, one formal death edge per previous detection and
    one formal birth edge per current detection.  No geometric or speed gate
    is applied.
    """

    if not isinstance(problem, GlobalAssignmentProblem):
        raise TypeError("solver vereist een GlobalAssignmentProblem")
    previous = tuple(sorted(problem.previous_detection_ids))
    current = tuple(sorted(problem.current_detection_ids))
    previous_count = len(previous)
    current_count = len(current)
    side = previous_count + current_count
    if side == 0:
        solution = AssignmentSolution(
            rank=1,
            total_cost=0.0,
            matches=(),
            deaths=(),
            births=(),
        )
        return GlobalAssignmentResult(
            problem=problem,
            solutions=(solution,),
            exact=True,
            maximum_current_detections=None,
            solver="exact_hungarian_one_best",
        )

    edge_cost = {
        (item.previous_detection_id, item.current_detection_id): item.cost
        for item in problem.edge_costs
    }
    death_cost = dict(problem.death_costs)
    birth_cost = dict(problem.birth_costs)
    matrix: list[list[float | None]] = [
        [None] * side for _item in range(side)
    ]

    # Previous rows can match every current column or their own death column.
    for previous_index, previous_id in enumerate(previous):
        for current_index, current_id in enumerate(current):
            matrix[previous_index][current_index] = edge_cost[
                (previous_id, current_id)
            ]
        death_column = current_count + previous_index
        matrix[previous_index][death_column] = death_cost[previous_id]

    # One birth row per current detection can claim its current column.  If a
    # previous row already claimed that detection, the birth row fills any
    # unused death-dummy column at zero cost.
    for current_index, current_id in enumerate(current):
        row_index = previous_count + current_index
        matrix[row_index][current_index] = birth_cost[current_id]
        for previous_index in range(previous_count):
            death_column = current_count + previous_index
            matrix[row_index][death_column] = 0.0

    chosen_columns = _solve_dense_perfect_assignment(matrix)
    matches: list[tuple[str, str]] = []
    deaths: list[str] = []
    matched_current: set[str] = set()
    for previous_index, previous_id in enumerate(previous):
        column_index = chosen_columns[previous_index]
        if column_index < current_count:
            current_id = current[column_index]
            matches.append((previous_id, current_id))
            matched_current.add(current_id)
        else:
            deaths.append(previous_id)
    births = tuple(
        current_id for current_id in current if current_id not in matched_current
    )
    total_cost = (
        sum(edge_cost[item] for item in matches)
        + sum(death_cost[item] for item in deaths)
        + sum(birth_cost[item] for item in births)
    )
    solution = AssignmentSolution(
        rank=1,
        total_cost=total_cost,
        matches=tuple(matches),
        deaths=tuple(deaths),
        births=births,
    )
    return GlobalAssignmentResult(
        problem=problem,
        solutions=(solution,),
        exact=True,
        maximum_current_detections=None,
        solver="exact_hungarian_one_best",
        explored_subproblems=1,
    )


@dataclass(frozen=True, slots=True)
class _MurtySubproblem:
    """One canonical semantic partition over previous-detection decisions."""

    partition_start: int
    fixed: tuple[tuple[int, int], ...]
    forbidden: tuple[tuple[int, int], ...]
    chosen_columns: tuple[int, ...]
    solution: AssignmentSolution


def solve_bounded_k_best_global_assignment(
    problem: GlobalAssignmentProblem,
    *,
    solution_limit: int,
    maximum_subproblems: int,
) -> GlobalAssignmentResult:
    """Return exact ordered k-best semantic decisions with a hard work cap.

    This is a deterministic Murty/Lawler partition over the decisions made by
    the *real previous-detection rows*.  Births are derived from unmatched
    current detections, so arbitrary dummy-row permutations cannot masquerade
    as distinct identity hypotheses.  No distance, displacement or speed is a
    forbidden edge.  If the caller-owned subproblem cap is reached before k
    exact solutions are established, ``capacity_exhausted`` is explicit and a
    caller must not advance state from the partial result.
    """

    if not isinstance(problem, GlobalAssignmentProblem):
        raise TypeError("solver vereist een GlobalAssignmentProblem")
    for label, value in (
        ("solution_limit", solution_limit),
        ("maximum_subproblems", maximum_subproblems),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise ValueError(f"{label} moet een positief geheel getal zijn")

    previous, current, base_matrix = _augmented_assignment_matrix(problem)
    if not previous:
        solution = AssignmentSolution(
            rank=1,
            total_cost=math.fsum(value for _item, value in problem.birth_costs),
            matches=(),
            deaths=(),
            births=current,
        )
        return GlobalAssignmentResult(
            problem=problem,
            solutions=(solution,),
            exact=True,
            maximum_current_detections=None,
            solver="bounded_exact_k_best_murty_hungarian",
            explored_subproblems=1,
            maximum_subproblems=maximum_subproblems,
            search_exhausted=True,
        )

    explored = 0

    def solve_partition(
        partition_start: int,
        fixed: tuple[tuple[int, int], ...],
        forbidden: tuple[tuple[int, int], ...],
    ) -> _MurtySubproblem | None:
        nonlocal explored
        if explored >= maximum_subproblems:
            return None
        explored += 1
        matrix = [list(row) for row in base_matrix]
        for row, column in forbidden:
            matrix[row][column] = None
        for row, column in fixed:
            if matrix[row][column] is None:
                return None
            for candidate_column in range(len(matrix)):
                if candidate_column != column:
                    matrix[row][candidate_column] = None
            for candidate_row in range(len(matrix)):
                if candidate_row != row:
                    matrix[candidate_row][column] = None
        try:
            chosen = _solve_dense_perfect_assignment(matrix)
        except (AssertionError, ValueError) as exc:
            if not any(
                marker in str(exc)
                for marker in (
                    "geen perfect matching",
                    "verboden edge",
                    "eindige geldige kosten",
                )
            ):
                raise
            return None
        solution = _semantic_solution(
            problem,
            previous=previous,
            current=current,
            chosen_columns=chosen,
            rank=1,
        )
        return _MurtySubproblem(
            partition_start=partition_start,
            fixed=fixed,
            forbidden=forbidden,
            chosen_columns=chosen,
            solution=solution,
        )

    root = solve_partition(0, (), ())
    if root is None:
        raise AssertionError("globale assignment leverde geen oplossing")
    queue: list[
        tuple[
            float,
            tuple[tuple[str, str], ...],
            tuple[str, ...],
            tuple[str, ...],
            int,
            _MurtySubproblem,
        ]
    ] = []
    serial = 0

    def enqueue(node: _MurtySubproblem) -> None:
        nonlocal serial
        serial += 1
        item = node.solution
        heapq.heappush(queue, (
            item.total_cost,
            item.matches,
            item.deaths,
            item.births,
            serial,
            node,
        ))

    enqueue(root)
    retained: list[AssignmentSolution] = []
    retained_decisions: set[
        tuple[tuple[tuple[str, str], ...], tuple[str, ...], tuple[str, ...]]
    ] = set()
    seen_partitions = {(root.partition_start, root.fixed, root.forbidden)}
    capacity_exhausted = False

    while queue and len(retained) < solution_limit:
        *_key, node = heapq.heappop(queue)
        decision = (
            node.solution.matches,
            node.solution.deaths,
            node.solution.births,
        )
        if decision not in retained_decisions:
            retained_decisions.add(decision)
            retained.append(node.solution)
        if len(retained) >= solution_limit:
            break

        fixed_map = dict(node.fixed)
        forbidden = set(node.forbidden)
        semantic_columns = node.chosen_columns[:len(previous)]
        for split_row in range(node.partition_start, len(previous)):
            child_fixed = dict(fixed_map)
            for prefix_row in range(node.partition_start, split_row):
                child_fixed[prefix_row] = semantic_columns[prefix_row]
            child_forbidden = set(forbidden)
            child_forbidden.add((split_row, semantic_columns[split_row]))
            fixed_tuple = tuple(sorted(child_fixed.items()))
            forbidden_tuple = tuple(sorted(child_forbidden))
            partition_key = (split_row, fixed_tuple, forbidden_tuple)
            if partition_key in seen_partitions:
                continue
            seen_partitions.add(partition_key)
            if explored >= maximum_subproblems:
                capacity_exhausted = True
                break
            child = solve_partition(split_row, fixed_tuple, forbidden_tuple)
            if child is not None:
                enqueue(child)
        if capacity_exhausted:
            break

    if not retained:
        raise AssertionError("globale assignment leverde geen behouden oplossing")
    retained.sort(key=lambda item: (
        item.total_cost,
        item.matches,
        item.deaths,
        item.births,
    ))
    canonical = tuple(
        AssignmentSolution(
            rank=rank,
            total_cost=item.total_cost,
            matches=item.matches,
            deaths=item.deaths,
            births=item.births,
        )
        for rank, item in enumerate(retained, start=1)
    )
    return GlobalAssignmentResult(
        problem=problem,
        solutions=canonical,
        exact=not capacity_exhausted,
        maximum_current_detections=None,
        solver="bounded_exact_k_best_murty_hungarian",
        explored_subproblems=explored,
        maximum_subproblems=maximum_subproblems,
        search_exhausted=(not queue and not capacity_exhausted),
        capacity_exhausted=capacity_exhausted,
    )


def _augmented_assignment_matrix(
    problem: GlobalAssignmentProblem,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[tuple[float | None, ...], ...]]:
    previous = tuple(sorted(problem.previous_detection_ids))
    current = tuple(sorted(problem.current_detection_ids))
    previous_count = len(previous)
    current_count = len(current)
    side = previous_count + current_count
    if side == 0:
        return previous, current, ()
    edge_cost = {
        (item.previous_detection_id, item.current_detection_id): item.cost
        for item in problem.edge_costs
    }
    death_cost = dict(problem.death_costs)
    birth_cost = dict(problem.birth_costs)
    matrix: list[list[float | None]] = [[None] * side for _item in range(side)]
    for previous_index, previous_id in enumerate(previous):
        for current_index, current_id in enumerate(current):
            matrix[previous_index][current_index] = edge_cost[(previous_id, current_id)]
        matrix[previous_index][current_count + previous_index] = death_cost[previous_id]
    for current_index, current_id in enumerate(current):
        row_index = previous_count + current_index
        matrix[row_index][current_index] = birth_cost[current_id]
        for previous_index in range(previous_count):
            matrix[row_index][current_count + previous_index] = 0.0
    return previous, current, tuple(tuple(row) for row in matrix)


def _semantic_solution(
    problem: GlobalAssignmentProblem,
    *,
    previous: tuple[str, ...],
    current: tuple[str, ...],
    chosen_columns: tuple[int, ...],
    rank: int,
) -> AssignmentSolution:
    edge_cost = {
        (item.previous_detection_id, item.current_detection_id): item.cost
        for item in problem.edge_costs
    }
    death_cost = dict(problem.death_costs)
    birth_cost = dict(problem.birth_costs)
    matches: list[tuple[str, str]] = []
    deaths: list[str] = []
    matched_current: set[str] = set()
    for row, previous_id in enumerate(previous):
        column = chosen_columns[row]
        if column < len(current):
            current_id = current[column]
            matches.append((previous_id, current_id))
            matched_current.add(current_id)
        else:
            deaths.append(previous_id)
    births = tuple(item for item in current if item not in matched_current)
    return AssignmentSolution(
        rank=rank,
        total_cost=math.fsum((
            *(edge_cost[item] for item in matches),
            *(death_cost[item] for item in deaths),
            *(birth_cost[item] for item in births),
        )),
        matches=tuple(matches),
        deaths=tuple(deaths),
        births=births,
    )


def solve_global_assignment_with_fallback(
    problem: GlobalAssignmentProblem,
    *,
    solution_limit: int,
    maximum_top_k_current_detections: int,
) -> GlobalAssignmentResult:
    """Use exact top-k when bounded, otherwise exact scalable one-best."""

    if (
        isinstance(solution_limit, bool)
        or not isinstance(solution_limit, int)
        or solution_limit <= 0
    ):
        raise ValueError("solution_limit moet een positief geheel getal zijn")
    if (
        isinstance(maximum_top_k_current_detections, bool)
        or not isinstance(maximum_top_k_current_detections, int)
        or maximum_top_k_current_detections < 0
    ):
        raise ValueError(
            "maximum_top_k_current_detections moet niet-negatief zijn"
        )
    if len(problem.current_detection_ids) <= maximum_top_k_current_detections:
        return solve_exact_global_assignment(
            problem,
            solution_limit=solution_limit,
            maximum_current_detections=maximum_top_k_current_detections,
        )
    return solve_global_assignment_one_best(problem)


def _solve_dense_perfect_assignment(
    costs: list[list[float | None]],
) -> tuple[int, ...]:
    """Return an exact deterministic square assignment using Hungarian duals.

    ``None`` represents a structurally forbidden augmented edge.  Valid costs
    are scaled before a finite forbidden sentinel is introduced, so arbitrary
    finite negative values and very large magnitudes remain supported without
    overflow.  The augmented birth/death construction always has a valid
    perfect assignment; selecting a sentinel is therefore an internal error.
    """

    size = len(costs)
    if size == 0 or any(len(row) != size for row in costs):
        raise ValueError("Hungarian assignment vereist een niet-lege matrix")
    finite_values = [
        value
        for row in costs
        for value in row
        if value is not None
    ]
    if not finite_values or any(
        not math.isfinite(value) for value in finite_values
    ):
        raise ValueError("assignmentmatrix vereist eindige geldige kosten")
    scale = max(1.0, *(abs(value) for value in finite_values))
    forbidden = float(4 * size + 1)
    matrix = [
        [
            value / scale if value is not None else forbidden
            for value in row
        ]
        for row in costs
    ]

    # Classic O(n^3) primal-dual Hungarian algorithm. Arrays are one-based to
    # keep the shortest augmenting-column implementation auditable.
    row_potential = [0.0] * (size + 1)
    column_potential = [0.0] * (size + 1)
    column_row = [0] * (size + 1)
    previous_column = [0] * (size + 1)
    for row in range(1, size + 1):
        column_row[0] = row
        minimum = [math.inf] * (size + 1)
        used = [False] * (size + 1)
        column = 0
        while True:
            used[column] = True
            active_row = column_row[column]
            delta = math.inf
            next_column = 0
            for candidate_column in range(1, size + 1):
                if used[candidate_column]:
                    continue
                reduced = (
                    matrix[active_row - 1][candidate_column - 1]
                    - row_potential[active_row]
                    - column_potential[candidate_column]
                )
                if reduced < minimum[candidate_column]:
                    minimum[candidate_column] = reduced
                    previous_column[candidate_column] = column
                if (
                    minimum[candidate_column] < delta
                    or (
                        minimum[candidate_column] == delta
                        and (
                            next_column == 0
                            or candidate_column < next_column
                        )
                    )
                ):
                    delta = minimum[candidate_column]
                    next_column = candidate_column
            if not math.isfinite(delta) or next_column == 0:
                raise AssertionError(
                    "augmented assignmentmatrix heeft geen perfect matching"
                )
            for candidate_column in range(size + 1):
                if used[candidate_column]:
                    row_potential[column_row[candidate_column]] += delta
                    column_potential[candidate_column] -= delta
                else:
                    minimum[candidate_column] -= delta
            column = next_column
            if column_row[column] == 0:
                break
        while True:
            predecessor = previous_column[column]
            column_row[column] = column_row[predecessor]
            column = predecessor
            if column == 0:
                break

    chosen = [-1] * size
    for column in range(1, size + 1):
        row = column_row[column]
        if row <= 0 or chosen[row - 1] != -1:
            raise AssertionError("Hungarian assignment is niet één-op-één")
        chosen[row - 1] = column - 1
    if any(column < 0 for column in chosen):
        raise AssertionError("Hungarian assignment dekt niet alle rijen")
    if any(
        costs[row][column] is None
        for row, column in enumerate(chosen)
    ):
        raise AssertionError("Hungarian assignment koos een verboden edge")
    return tuple(chosen)


def _retain_partial(
    states: dict[int, list[_PartialSolution]],
    mask: int,
    candidate: _PartialSolution,
    solution_limit: int,
) -> None:
    values = states.setdefault(mask, [])
    values.append(candidate)
    values.sort(
        key=lambda item: (item.cost, item.matches, item.deaths)
    )
    del values[solution_limit:]


def _validate_identifiers(values: tuple[str, ...], label: str) -> None:
    if any(not isinstance(item, str) or not item for item in values):
        raise ValueError(f"{label} detection-ID's moeten niet-leeg zijn")
    if len(values) != len(set(values)):
        raise ValueError(f"{label} detection-ID's moeten uniek zijn")


def _validate_event_costs(
    values: Iterable[tuple[str, float]],
    expected_ids: tuple[str, ...],
    label: str,
) -> None:
    items = tuple(values)
    identifiers = [identifier for identifier, _cost in items]
    if len(identifiers) != len(set(identifiers)):
        raise ValueError(f"dubbele {label} cost")
    if set(identifiers) != set(expected_ids):
        raise ValueError(f"{label} costs dekken niet exact alle detecties")
    if any(not math.isfinite(cost) for _identifier, cost in items):
        raise ValueError(f"{label} costs moeten eindig zijn")
