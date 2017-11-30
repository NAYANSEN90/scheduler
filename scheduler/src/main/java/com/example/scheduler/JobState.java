package com.example.scheduler;

public enum JobState {
    QUEUED("QUEUED"),
    RUNNING("RUNNING"),
    SUCCEEDED("SUCCEEDED"),
    FAILED("FAILED");

    private String _state;

    JobState(String state){
        this._state = state;
    }

    @Override
    public String toString(){
        return _state;
    }
}
