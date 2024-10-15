import math

import process
from des import SchedulerDES
from event import Event, EventTypes
from process import ProcessStates


class FCFS(SchedulerDES):
    def scheduler_func(self, cur_event):
        # FCFS: In this scheduler we select the process based on arrival time
        index = cur_event.process_id
        return self.processes[index]

    def dispatcher_func(self, cur_process):
        # Execute the process until it's complete
        execute_process = cur_process.run_for(cur_process.remaining_time, self.time)

        # Process is complete so set the state of the current process to TERMINATED
        # Create and return an event to indicate that the CPU has finished processing the current process
        cur_process.process_state = ProcessStates.TERMINATED
        return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_DONE,
                     event_time=self.time + execute_process + self.context_switch_time)


class SJF(SchedulerDES):
    def scheduler_func(self, cur_event):
        # SJF: In this scheduler we select the process with the minimum service time
        processes_ready = [p for p in self.processes if p.process_state == ProcessStates.READY]
        return min(processes_ready, key=lambda processes: processes.service_time)

    def dispatcher_func(self, cur_process):
        # Execute the process until it's complete and create an event to indicate completion of the process
        execute_process = cur_process.run_for(cur_process.remaining_time, self.time)

        cur_process.process_state = ProcessStates.TERMINATED
        return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_DONE,
                     event_time=self.time + execute_process + self.context_switch_time)


class RR(SchedulerDES):
    def scheduler_func(self, cur_event):
        # RR: In this scheduler we select the next process in the queue
        index = cur_event.process_id
        return self.processes[index]

    def dispatcher_func(self, cur_process):
        # Execute the process for a fixed quantum
        execute_process = cur_process.run_for(self.quantum, self.time)

        if cur_process.remaining_time > 0:
            # Process is not complete yet so creates an event to indicate that the CPU hasn't
            # finished processing the current process and adds the process back to the queue
            return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_REQ,
                         event_time=self.time + execute_process + self.context_switch_time)
        else:
            # Process has completed so creates an event like previously
            cur_process.process_state = ProcessStates.TERMINATED
            return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_DONE,
                         event_time=self.time + execute_process + self.context_switch_time)


class SRTF(SchedulerDES):
    def scheduler_func(self, cur_event):
        # SRTF: In this scheduler we select the process with the shortest remaining time
        processes_ready = [p for p in self.processes if p.process_state == ProcessStates.READY]
        return min(processes_ready, key=lambda processes: processes.remaining_time)

    def dispatcher_func(self, cur_process):
        # Execute the process for a time slice/quantum
        time_slice = min(self.next_event_time() - self.time, cur_process.remaining_time)
        execute_process = cur_process.run_for(time_slice, self.time)

        if cur_process.remaining_time > 0:
            # If the process needs more time add it back to the queue
            return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_REQ,
                         event_time=self.time + execute_process + self.context_switch_time)
        else:
            # CPU has finished executing the process so set the process state to TERMINATED then create and return an
            # event to indicate completion
            cur_process.process_state = ProcessStates.TERMINATED
            return Event(process_id=cur_process.process_id, event_type=EventTypes.PROC_CPU_DONE,
                         event_time=self.time + execute_process + self.context_switch_time)
