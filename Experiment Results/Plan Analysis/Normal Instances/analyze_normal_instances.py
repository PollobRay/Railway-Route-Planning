#!/usr/bin/env python3
"""
Railway Planner Log Analyzer for Normal Instances (p1-p100)

Analyzes planner log files for normal (non-disrupted) railway planning instances.
Provides summaries per 10 instances (e.g., p1-p10, p11-p20, etc.)

Metrics Extracted:
- Quality Metrics: Makespan, Plan Length, Total Delay (always 0 for normal instances)
- Computation Metrics: Generation Time, States Evaluated
- Delay breakdown (all zeros for normal instances)

Author: Railway Planning Research
Version: 2.0 - Normal Instances Focus
"""

import os
import re
import argparse
import json
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
from collections import defaultdict


@dataclass
class PlanAction:
    """Represents a single action in the plan."""
    start_time: float
    action_name: str
    parameters: List[str]
    duration: float
    
    @property
    def end_time(self) -> float:
        return self.start_time + self.duration


@dataclass
class PlannerResult:
    """Stores results from a single planner run."""
    instance: str
    planner: str
    solved: bool = False
    makespan: float = 0.0
    plan_length: int = 0
    states_evaluated: int = 0
    time_seconds: float = 0.0
    actions: List[PlanAction] = field(default_factory=list)
    
    # Delay breakdown (all zero for normal instances)
    total_delay: float = 0.0
    blockage_delay: float = 0.0
    slowdown_delay: float = 0.0
    engine_repair_delay: float = 0.0
    
    # Memory/timeout issues
    memory_error: bool = False
    timeout: bool = False


@dataclass
class ProblemInfo:
    """Stores problem instance information."""
    num_trains: int = 0
    num_stations: int = 0
    num_track_points: int = 0
    distances: Dict[Tuple[str, str], float] = field(default_factory=dict)
    slowdowns: Dict[Tuple[str, str], float] = field(default_factory=dict)
    train_speeds: Dict[str, float] = field(default_factory=dict)
    train_gauges: Dict[str, str] = field(default_factory=dict)
    boarding_times: Dict[Tuple[str, str], float] = field(default_factory=dict)


class PlannerLogAnalyzer:
    """Analyzes OPTIC and POPF planner log files for normal instances."""
    
    def __init__(self, directory: str):
        self.directory = directory
        self.results: Dict[str, Dict[str, PlannerResult]] = defaultdict(dict)
        self.problem_info: Dict[str, ProblemInfo] = {}
        
    def parse_problem_file(self, problem_path: str) -> ProblemInfo:
        """Parse PDDL problem file to extract distances, speeds, and other info."""
        info = ProblemInfo()
        
        if not os.path.exists(problem_path):
            return info
            
        with open(problem_path, 'r') as f:
            content = f.read()
        
        # Extract distances
        distance_pattern = r'\(= \(distance (p\d+) (p\d+)\) (\d+(?:\.\d+)?)\)'
        for match in re.finditer(distance_pattern, content):
            p1, p2, dist = match.groups()
            info.distances[(p1, p2)] = float(dist)
        
        # Extract slowdown factors (should be 0.0 for normal instances)
        slowdown_pattern = r'\(= \(slowdown (p\d+) (p\d+)\) (\d+(?:\.\d+)?)\)'
        for match in re.finditer(slowdown_pattern, content):
            p1, p2, sd = match.groups()
            info.slowdowns[(p1, p2)] = float(sd)
        
        # Extract train speeds
        speed_pattern = r'\(= \(train-speed (t\d+)\) (\d+(?:\.\d+)?)\)'
        for match in re.finditer(speed_pattern, content):
            train, speed = match.groups()
            info.train_speeds[train] = float(speed)
        
        # Extract train gauges
        gauge_pattern = r'\(train-gauge (t\d+) (\w+)\)'
        for match in re.finditer(gauge_pattern, content):
            train, gauge = match.groups()
            info.train_gauges[train] = gauge
        
        # Extract boarding times
        boarding_pattern = r'\(= \(boarding-time (t\d+) (s\d+)\) (\d+(?:\.\d+)?)\)'
        for match in re.finditer(boarding_pattern, content):
            train, station, bt = match.groups()
            info.boarding_times[(train, station)] = float(bt)
        
        # Count trains
        train_pattern = r'\(train-at (t\d+)'
        trains = set(re.findall(train_pattern, content))
        info.num_trains = len(trains)
        
        # Count stations
        station_pattern = r'\(platform-at p\d+ (s\d+)\)'
        stations = set(re.findall(station_pattern, content))
        info.num_stations = len(stations)
        
        # Count track points
        tp_pattern = r'(p\d+)\s'
        objects_section = re.search(r'\(:objects(.*?)\)', content, re.DOTALL)
        if objects_section:
            track_points = set(re.findall(r'(p\d+)', objects_section.group(1)))
            info.num_track_points = len(track_points)
        
        return info
    
    def parse_popf_log(self, log_path: str, instance: str) -> PlannerResult:
        """Parse POPF planner log file."""
        result = PlannerResult(instance=instance, planner='POPF')
        
        if not os.path.exists(log_path):
            return result
            
        with open(log_path, 'r') as f:
            content = f.read()
        
        # Check for memory error
        if 'std::bad_alloc' in content or 'bad_alloc' in content:
            result.memory_error = True
            return result
        
        # Check for timeout
        if 'timeout' in content.lower():
            result.timeout = True
            return result
        
        # Check if solution was found
        if ';;;; Solution Found' not in content:
            if not re.search(r'^\d+\.\d+:\s*\(', content, re.MULTILINE):
                return result
        
        result.solved = True
        
        # Extract states evaluated
        states_match = re.search(r'; States evaluated:\s*(\d+)', content)
        if states_match:
            result.states_evaluated = int(states_match.group(1))
        
        # Extract time
        time_match = re.search(r'; Time\s+(\d+(?:\.\d+)?)', content)
        if time_match:
            result.time_seconds = float(time_match.group(1))
        
        # Extract makespan (Cost)
        cost_match = re.search(r'; Cost:\s*(\d+(?:\.\d+)?)', content)
        if cost_match:
            result.makespan = float(cost_match.group(1))
        
        # Extract plan actions
        solution_idx = content.rfind(';;;; Solution Found')
        if solution_idx != -1:
            plan_content = content[solution_idx:]
        else:
            plan_content = content
        
        action_pattern = r'^(\d+(?:\.\d+)?):\s*\(([^)]+)\)\s*\[(\d+(?:\.\d+)?)\]'
        for match in re.finditer(action_pattern, plan_content, re.MULTILINE):
            start_time = float(match.group(1))
            action_str = match.group(2).strip()
            duration = float(match.group(3))
            
            parts = action_str.split()
            action_name = parts[0] if parts else ''
            parameters = parts[1:] if len(parts) > 1 else []
            
            result.actions.append(PlanAction(
                start_time=start_time,
                action_name=action_name,
                parameters=parameters,
                duration=duration
            ))
        
        result.plan_length = len(result.actions)
        
        if result.makespan == 0.0 and result.actions:
            result.makespan = max(a.end_time for a in result.actions)
        
        return result
    
    def calculate_delays(self, result: PlannerResult, problem_info: ProblemInfo) -> None:
        """
        Calculate total delay for normal instances.
        
        For normal (non-disrupted) instances:
        - Slowdown factors are 0.0, so slowdown_delay = 0.0
        - No blocked trains, so blockage_delay = 0.0
        - No engine damage, so engine_repair_delay = 0.0
        - Total delay = 0.0
        
        The method still processes the plan to verify no disruptions exist.
        """
        if not result.solved or not result.actions:
            return
        
        total_delay = 0.0
        slowdown_delay = 0.0
        blockage_delay = 0.0
        engine_repair_delay = 0.0
        
        for action in result.actions:
            # Calculate slowdown delay for drive actions
            if action.action_name in ['drive-train', 'drive-assisted-train']:
                if action.action_name == 'drive-train' and len(action.parameters) >= 3:
                    train, from_pt, to_pt = action.parameters[0], action.parameters[1], action.parameters[2]
                elif action.action_name == 'drive-assisted-train' and len(action.parameters) >= 4:
                    train, from_pt, to_pt = action.parameters[1], action.parameters[2], action.parameters[3]
                else:
                    continue
                
                # Get distance and slowdown
                distance = problem_info.distances.get((from_pt, to_pt), 0)
                if distance == 0:
                    distance = problem_info.distances.get((to_pt, from_pt), 0)
                
                slowdown = problem_info.slowdowns.get((from_pt, to_pt), 0)
                if slowdown == 0:
                    slowdown = problem_info.slowdowns.get((to_pt, from_pt), 0)
                
                speed = problem_info.train_speeds.get(train, 1.0)
                
                # Calculate theoretical time without slowdown
                if distance > 0 and speed > 0:
                    normal_time = distance / speed
                    # For normal instances slowdown should be 0.0
                    if slowdown > 0 and slowdown < 1:
                        actual_time = distance / (speed * (1 - slowdown))
                        delay_from_slowdown = actual_time - normal_time
                        slowdown_delay += delay_from_slowdown
            
            # Track blockage resolution delay (should not occur in normal instances)
            elif action.action_name == 'resolve-train-blockage':
                blockage_delay += action.duration
            
            # Track clearing delay (should not occur in normal instances)
            elif action.action_name == 'clear-blocked-track':
                blockage_delay += action.duration
            
            # Engine repair related delays (should not occur in normal instances)
            elif action.action_name in ['drive-engine-to-damaged-up-train', 
                                        'drive-engine-to-damaged-down-train',
                                        'attach-engine']:
                engine_repair_delay += action.duration
        
        result.slowdown_delay = round(slowdown_delay, 3)
        result.blockage_delay = round(blockage_delay, 3)
        result.engine_repair_delay = round(engine_repair_delay, 3)
        result.total_delay = round(slowdown_delay + blockage_delay + engine_repair_delay, 3)
    
    def analyze_all(self) -> None:
        """Analyze all normal instance log files (p1-p100)."""
        for i in range(1, 101):
            instance = f'p{i}'
            
            # Parse problem file
            problem_path = os.path.join(self.directory, f'{instance}.pddl')
            self.problem_info[instance] = self.parse_problem_file(problem_path)
            
            # Parse POPF log
            popf_log = os.path.join(self.directory, f'Popf-{instance}.txt')
            popf_result = self.parse_popf_log(popf_log, instance)
            self.calculate_delays(popf_result, self.problem_info[instance])
            self.results[instance]['POPF'] = popf_result
        
        solved_count = sum(
            1 for inst in self.results.values()
            if 'POPF' in inst and inst['POPF'].solved
        )
        print(f"Analyzed {len(self.results)} normal instances ({solved_count} solved)")
    
    def get_batch_metrics(self, start: int, end: int, planner: str = 'POPF') -> Dict:
        """Get aggregated metrics for a batch of instances."""
        instances = [f'p{i}' for i in range(start, end + 1)]
        
        metrics = {
            'total': 0,
            'solved': 0,
            'makespan_sum': 0.0,
            'makespan_avg': 0.0,
            'plan_length_sum': 0,
            'plan_length_avg': 0.0,
            'total_delay_sum': 0.0,
            'slowdown_delay_sum': 0.0,
            'blockage_delay_sum': 0.0,
            'engine_delay_sum': 0.0,
            'time_sum': 0.0,
            'time_avg': 0.0,
            'states_sum': 0,
            'states_avg': 0.0,
            'memory_errors': 0,
            'timeouts': 0
        }
        
        for inst in instances:
            metrics['total'] += 1
            if inst in self.results and planner in self.results[inst]:
                r = self.results[inst][planner]
                if r.solved:
                    metrics['solved'] += 1
                    metrics['makespan_sum'] += r.makespan
                    metrics['plan_length_sum'] += r.plan_length
                    metrics['total_delay_sum'] += r.total_delay
                    metrics['slowdown_delay_sum'] += r.slowdown_delay
                    metrics['blockage_delay_sum'] += r.blockage_delay
                    metrics['engine_delay_sum'] += r.engine_repair_delay
                    metrics['time_sum'] += r.time_seconds
                    metrics['states_sum'] += r.states_evaluated
                
                if r.memory_error:
                    metrics['memory_errors'] += 1
                if r.timeout:
                    metrics['timeouts'] += 1
        
        # Calculate averages
        if metrics['solved'] > 0:
            metrics['makespan_avg'] = metrics['makespan_sum'] / metrics['solved']
            metrics['plan_length_avg'] = metrics['plan_length_sum'] / metrics['solved']
            metrics['time_avg'] = metrics['time_sum'] / metrics['solved']
            metrics['states_avg'] = metrics['states_sum'] / metrics['solved']
        
        return metrics
    
    def generate_batch_summary_table(self) -> str:
        """Generate summary table grouped by batches of 10."""
        lines = []
        lines.append('=' * 120)
        lines.append('NORMAL INSTANCES SUMMARY (p1-p100) - GROUPED BY 10')
        lines.append('=' * 120)
        lines.append('')
        
        # Header
        header = f"{'Batch':<15} {'Solved':<10} {'Avg MS':<10} {'Avg PL':<10} " \
                f"{'Total Delay':<12} {'SD Delay':<10} {'BL Delay':<10} {'ER Delay':<10} " \
                f"{'Avg Time':<10} {'Avg States':<12}"
        lines.append(header)
        lines.append('-' * 120)
        
        # Data rows for each batch of 10
        for start in range(1, 101, 10):
            end = min(start + 9, 100)
            batch_name = f"p{start}-p{end}"
            
            metrics = self.get_batch_metrics(start, end, 'POPF')
            
            row = f"{batch_name:<15} " \
                  f"{metrics['solved']}/{metrics['total']:<7} " \
                  f"{metrics['makespan_avg']:<10.2f} " \
                  f"{metrics['plan_length_avg']:<10.1f} " \
                  f"{metrics['total_delay_sum']:<12.2f} " \
                  f"{metrics['slowdown_delay_sum']:<10.2f} " \
                  f"{metrics['blockage_delay_sum']:<10.2f} " \
                  f"{metrics['engine_delay_sum']:<10.2f} " \
                  f"{metrics['time_avg']:<10.3f} " \
                  f"{metrics['states_avg']:<12.1f}"
            
            lines.append(row)
        
        lines.append('=' * 120)
        lines.append('')
        lines.append('Legend:')
        lines.append('  MS = Makespan, PL = Plan Length')
        lines.append('  SD Delay = Slowdown Delay, BL Delay = Blockage Delay, ER Delay = Engine Repair Delay')
        lines.append('  Note: For normal (non-disrupted) instances, all delay values should be 0.00')
        lines.append('')
        
        return '\n'.join(lines)
    
    def generate_detailed_csv(self) -> str:
        """Generate detailed CSV with all instances."""
        lines = []
        
        # Header
        lines.append('Instance,Solved,Makespan,PlanLength,TotalDelay,SlowdownDelay,'
                    'BlockageDelay,EngineRepairDelay,Time,States,MemoryError,Timeout')
        
        # Data rows
        for i in range(1, 101):
            instance = f'p{i}'
            
            if instance in self.results and 'POPF' in self.results[instance]:
                r = self.results[instance]['POPF']
                
                lines.append(f"{instance},"
                           f"{1 if r.solved else 0},"
                           f"{r.makespan:.3f},"
                           f"{r.plan_length},"
                           f"{r.total_delay:.3f},"
                           f"{r.slowdown_delay:.3f},"
                           f"{r.blockage_delay:.3f},"
                           f"{r.engine_repair_delay:.3f},"
                           f"{r.time_seconds:.3f},"
                           f"{r.states_evaluated},"
                           f"{1 if r.memory_error else 0},"
                           f"{1 if r.timeout else 0}")
            else:
                lines.append(f"{instance},0,0.0,0,0.0,0.0,0.0,0.0,0.0,0,0,0")
        
        return '\n'.join(lines)
    
    def generate_overall_summary(self) -> str:
        """Generate overall summary statistics."""
        lines = []
        lines.append('=' * 80)
        lines.append('OVERALL SUMMARY - NORMAL INSTANCES (p1-p100)')
        lines.append('=' * 80)
        lines.append('')
        
        # Overall metrics
        metrics = self.get_batch_metrics(1, 100, 'POPF')
        
        lines.append(f"Total Instances: {metrics['total']}")
        lines.append(f"Solved: {metrics['solved']} ({100*metrics['solved']/max(metrics['total'], 1):.1f}%)")
        lines.append(f"Memory Errors: {metrics['memory_errors']}")
        lines.append(f"Timeouts: {metrics['timeouts']}")
        lines.append('')
        
        lines.append("Performance Metrics (Solved Instances):")
        lines.append(f"  Total Makespan: {metrics['makespan_sum']:.2f}")
        lines.append(f"  Average Makespan: {metrics['makespan_avg']:.2f}")
        lines.append(f"  Total Plan Length: {metrics['plan_length_sum']}")
        lines.append(f"  Average Plan Length: {metrics['plan_length_avg']:.1f}")
        lines.append('')
        
        lines.append("Delay Breakdown (Expected: all zeros for normal instances):")
        lines.append(f"  Total Delay: {metrics['total_delay_sum']:.2f}")
        lines.append(f"    - Slowdown Delay: {metrics['slowdown_delay_sum']:.2f}")
        lines.append(f"    - Blockage Delay: {metrics['blockage_delay_sum']:.2f}")
        lines.append(f"    - Engine Repair Delay: {metrics['engine_delay_sum']:.2f}")
        lines.append('')
        
        lines.append("Computation Metrics:")
        lines.append(f"  Total Time: {metrics['time_sum']:.2f}s")
        lines.append(f"  Average Time: {metrics['time_avg']:.3f}s")
        lines.append(f"  Total States: {metrics['states_sum']}")
        lines.append(f"  Average States: {metrics['states_avg']:.1f}")
        lines.append('')
        
        return '\n'.join(lines)
    
    def save_results(self, output_dir: str) -> None:
        """Save all results to files."""
        os.makedirs(output_dir, exist_ok=True)
        
        # Save batch summary table
        with open(os.path.join(output_dir, 'batch_summary.txt'), 'w') as f:
            f.write(self.generate_batch_summary_table())
        
        # Save overall summary
        with open(os.path.join(output_dir, 'overall_summary.txt'), 'w') as f:
            f.write(self.generate_overall_summary())
        
        # Save detailed CSV
        with open(os.path.join(output_dir, 'detailed_results.csv'), 'w') as f:
            f.write(self.generate_detailed_csv())
        
        # Save JSON data for further processing
        json_data = {}
        
        for i in range(1, 101):
            inst = f'p{i}'
            if inst in self.results and 'POPF' in self.results[inst]:
                r = self.results[inst]['POPF']
                json_data[inst] = {
                    'solved': r.solved,
                    'makespan': r.makespan,
                    'plan_length': r.plan_length,
                    'total_delay': r.total_delay,
                    'slowdown_delay': r.slowdown_delay,
                    'blockage_delay': r.blockage_delay,
                    'engine_repair_delay': r.engine_repair_delay,
                    'time': r.time_seconds,
                    'states': r.states_evaluated,
                    'memory_error': r.memory_error,
                    'timeout': r.timeout
                }
        
        with open(os.path.join(output_dir, 'results_data.json'), 'w') as f:
            json.dump(json_data, f, indent=2)
        
        print(f'\nResults saved to {output_dir}/')
        print(f'  - batch_summary.txt (Summary per 10 instances)')
        print(f'  - overall_summary.txt (Overall statistics)')
        print(f'  - detailed_results.csv (All instance details)')
        print(f'  - results_data.json (JSON data)')


def main():
    parser = argparse.ArgumentParser(
        description='Analyze POPF planner logs for normal railway instances (p1-p100)'
    )
    parser.add_argument(
        '--directory', '-d',
        default='.',
        help='Directory containing log files and problem instances'
    )
    parser.add_argument(
        '--output', '-o',
        default='./analysis_results_normal',
        help='Output directory for results'
    )
    
    args = parser.parse_args()
    
    print('=' * 60)
    print('Railway Planner Log Analyzer - Normal Instances')
    print('Analyzing: p1 to p100')
    print('=' * 60)
    print(f'Input directory: {args.directory}')
    print(f'Output directory: {args.output}')
    print('=' * 60)
    
    analyzer = PlannerLogAnalyzer(args.directory)
    
    print('\nAnalyzing log files...')
    analyzer.analyze_all()
    
    # Save results
    analyzer.save_results(args.output)
    
    # Print summaries to console
    print('\n' + analyzer.generate_batch_summary_table())
    print('\n' + analyzer.generate_overall_summary())
    
    print('\nAnalysis complete!')


if __name__ == '__main__':
    main()
