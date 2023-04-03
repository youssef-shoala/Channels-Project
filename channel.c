#include "channel.h"

// Creates a new channel with the provided size and returns it to the caller
// A 0 size indicates an unbuffered channel, whereas a positive size indicates a buffered channel
channel_t* channel_create(size_t size)
{ 
    //Unbuffered channel (Sender blocks until receiver has recieved the value)

    //initialize channel and mutex in heap
    pthread_mutex_t *lockp = malloc(sizeof(pthread_mutex_t));
    pthread_mutex_init(lockp, NULL);    
    channel_t *channelp = malloc(sizeof(channel_t));
    channelp -> buffer = buffer_create(size); 
    channelp -> lockp = lockp; 
    pthread_cond_init(&channelp->is_full_cond, NULL);
    channelp -> is_full = false; 
    pthread_cond_init(&channelp->is_empty_cond, NULL);
    channelp -> is_empty = true; 
    channelp -> is_closed = false;

    //initialize select vars to null
    channelp -> select_lockp = NULL; 
    channelp -> select_condp = NULL; 
    channelp -> any_channel_selectedp = NULL; 


    return channelp;
}

//Helper function that will update the is_full and is_empty in a channel object
//Input: Channel obj pointer, Output: void
void update_channel(channel_t *channel)
{
    if (buffer_current_size(channel->buffer) == buffer_capacity(channel->buffer))
    {
        channel -> is_full = true; 
    }
    else
    {
        channel -> is_full = false;
    }

    if(buffer_current_size(channel->buffer) == 0)
    {
        channel -> is_empty = true;
    }
    else
    {
        channel -> is_empty = false;
    }
}

// Writes data to the given channel
// This is a blocking call i.e., the function only returns on a successful completion of send
// In case the channel is full, the function waits till the channel has space to write the new data
// Returns SUCCESS for successfully writing data to the channel,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_send(channel_t *channel, void* data)
{
    //lock the channels mutex so that other threads cannot access data
    pthread_mutex_lock(channel -> lockp);
    //if the channel is closed, unlock mutex and return CLOSED_ERROR
    if(channel -> is_closed)
    {
        pthread_mutex_unlock(channel -> lockp);
        return CLOSED_ERROR; 
    }
    //if the channel buffer is full, unlock it until something is removed from the buffer
    while(channel->is_full)
    {
        pthread_cond_wait(&channel -> is_full_cond, channel -> lockp);
        if(channel -> is_closed)
        {
            pthread_mutex_unlock(channel -> lockp);
            return CLOSED_ERROR; 
        }

    }
    //at this point of the code, the mutex is locked and there is space in the buffer
    bool action_performed = false;
    if (buffer_current_size(channel->buffer) < buffer_capacity(channel->buffer)) //should always be true if threadsafe
    {
        if (buffer_add(channel->buffer, data) == BUFFER_ERROR)
        {
            pthread_mutex_unlock(channel -> lockp);
            return GEN_ERROR;
        }
        //we will first signal the select condp if it exists, then wait for the channel select code to complete before signalling something else
        if(channel->select_lockp != NULL)
        {
            pthread_cond_signal(channel->select_condp); 

            pthread_mutex_lock(channel->select_lockp);
            pthread_mutex_unlock(channel->select_lockp);

            if(*(channel->any_channel_selectedp) == false)
            {
                pthread_cond_signal(&channel->is_empty_cond);
            }
        }
        else
        {
            pthread_cond_signal(&channel->is_empty_cond);
        }
        //if there are one or more threads waiting for something to be added to the buffer, this will trigger one of them to lock and continue their execution. NOTE: this is only signalled if the select signal wasnt successful
        update_channel(channel);
        action_performed = true;
    }
    //now we have added the data to the buffer and we can unlock the mutex
    pthread_mutex_unlock(channel -> lockp);
    //only return success if the if statement with the intended action is entered, otherwise something changed, not thread safe, return error
    if (action_performed) return SUCCESS;
    else return GEN_ERROR;
}

// Reads data from the given channel and stores it in the function's input parameter, data (Note that it is a double pointer)
// This is a blocking call i.e., the function only returns on a successful completion of receive
// In case the channel is empty, the function waits till the channel has some data to read
// Returns SUCCESS for successful retrieval of data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_receive(channel_t* channel, void** data)
{ 
    //lock the channels mutex so that other threads cannot access data
    pthread_mutex_lock(channel -> lockp);
    //if channel is closed, unlock mutex and return CLOSED_ERROR
    if(channel -> is_closed)
    {
        pthread_mutex_unlock(channel -> lockp);
        return CLOSED_ERROR; 
    }
    //if channel buffer is empty, unlock it until something is added to the buffer
    while(channel->is_empty)
    {
        pthread_cond_wait(&channel -> is_empty_cond, channel -> lockp);
        if(channel -> is_closed)
        {
            pthread_mutex_unlock(channel -> lockp);
            return CLOSED_ERROR; 
        }
    }
    //at this point of the code, the mutex is locked and there is atleast one item in the buffer
    bool action_performed = false; 
    if (buffer_current_size(channel->buffer) > 0) //should always be true if threadsafe
    {
        if (buffer_remove(channel -> buffer, data) == BUFFER_ERROR)
        {
            pthread_mutex_unlock(channel -> lockp);
            return GEN_ERROR;
        }
        //we will first signal the select condp if it exists, then wait for the channel select code to complete before signalling something else
        if(channel->select_lockp != NULL)
        {
            pthread_cond_signal(channel->select_condp); 

            pthread_mutex_lock(channel->select_lockp); 
            pthread_mutex_unlock(channel->select_lockp); 

            if(*(channel->any_channel_selectedp) == false)
            {
                pthread_cond_signal(&channel->is_full_cond);
            }
        }
        else
        {
            pthread_cond_signal(&channel->is_full_cond);
        }
        //if there are one or more threads waiting for something to be added to the buffer, this will trigger one of them to lock and continue their execution
        update_channel(channel);
        action_performed = true;
    }
    //now we have removed the data and returned it to the data pointer argument and we can unlock the mutex
    pthread_mutex_unlock(channel -> lockp);
    //only return success if the if statement with the intended action is entered, otherwise something changed, not thread safe, return error
    if (action_performed) return SUCCESS;
    else return GEN_ERROR;
}

// Writes data to the given channel
// This is a non-blocking call i.e., the function simply returns if the channel is full
// Returns SUCCESS for successfully writing data to the channel,
// CHANNEL_FULL if the channel is full and the data was not added to the buffer,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_send(channel_t* channel, void* data)
{
    
    //lock the channels mutex so that other threads cannot access the data
    pthread_mutex_lock(channel -> lockp);
    //if the channel is closed, we return CLOSED_ERROR and unlock mutex
    if(channel -> is_closed)
    {
        pthread_mutex_unlock(channel->lockp);
        return CLOSED_ERROR; 
    }
    //if the channel buffer is full, we return CHANNEL_FULL and unlock mutex (non blocking)
    if(channel->is_full)
    {
        pthread_mutex_unlock(channel->lockp);
        return CHANNEL_FULL;
    }
    //at this point of the code, mutex is still locked (no changes/no cond var) 
    if(buffer_add(channel->buffer, data) == BUFFER_ERROR)
    {
        pthread_mutex_unlock(channel->lockp);
        return GEN_ERROR; 
    }
    if(channel->select_lockp != NULL)
    {
        pthread_cond_signal(channel->select_condp); 

        pthread_mutex_lock(channel->select_lockp); 
        pthread_mutex_unlock(channel->select_lockp); 

        if(*(channel->any_channel_selectedp) == false)
        {
            pthread_cond_signal(&channel->is_empty_cond);
        }
    }
    else
    {
        pthread_cond_signal(&channel->is_empty_cond);
    }
    //we've edited the shared data, so update the channel
    update_channel(channel);
    //signal any threads waiting b/c of an empty buffer
    //we are finished with the data so unlock
    pthread_mutex_unlock(channel->lockp);
    //there is no change in the lock status since it's non blocking so no need for signaling or action verification
    return SUCCESS;
}

// Reads data from the given channel and stores it in the function's input parameter data (Note that it is a double pointer)
// This is a non-blocking call i.e., the function simply returns if the channel is empty
// Returns SUCCESS for successful retrieval of data,
// CHANNEL_EMPTY if the channel is empty and nothing was stored in data,
// CLOSED_ERROR if the channel is closed, and
// GEN_ERROR on encountering any other generic error of any sort
enum channel_status channel_non_blocking_receive(channel_t* channel, void** data)
{ 
    //lock the channels mutex so that other threads cannot access the data
    pthread_mutex_lock(channel->lockp);
    //if the channel is closed, we return CLOSED_ERROR and unlock mutex
    if(channel -> is_closed)
    {
        pthread_mutex_unlock(channel->lockp);
        return CLOSED_ERROR; 
    }
    //if the channel buffer is empty, we return CHANNEL_EMPTY and unlock mutex (non blocking)
    if(channel->is_empty)
    {
        pthread_mutex_unlock(channel->lockp);
        return CHANNEL_EMPTY; 
    }    
    //at this point of the code, mutex is still locked (no changes/no cond var)
    if(buffer_remove(channel->buffer, data) == BUFFER_ERROR)
    {
        pthread_mutex_unlock(channel->lockp);
        return GEN_ERROR; 
    }
    if(channel->select_lockp != NULL)
    {
        pthread_cond_signal(channel->select_condp); 

        pthread_mutex_lock(channel->select_lockp); 
        pthread_mutex_unlock(channel->select_lockp); 

        if(*(channel->any_channel_selectedp) == false)
        {
            pthread_cond_signal(&channel->is_full_cond);
        }
    }
    else
    {
        pthread_cond_signal(&channel->is_full_cond);
    }
    //we've edited the shared data, so update the channel
    update_channel(channel);
    //signal any threads waiting b/c of a full buffer
    //we are finished with the data so unlock 
    pthread_mutex_unlock(channel->lockp);
    //there is no change in the lock status since it's non blocking so no need for signaling or action verification
    return SUCCESS;
}

// Closes the channel and informs all the blocking send/receive/select calls to return with CLOSED_ERROR
// Once the channel is closed, send/receive/select operations will cease to function and just return CLOSED_ERROR
// Returns SUCCESS if close is successful,
// CLOSED_ERROR if the channel is already closed, and
// GEN_ERROR in any other error case
enum channel_status channel_close(channel_t* channel)
{
    
    //lock the channels mutex so that other threads cannot access the data
    pthread_mutex_lock(channel->lockp);
    //if channel is already closed, unlock and return CLOSED_ERROR
    if(channel -> is_closed)
    {
        pthread_mutex_unlock(channel -> lockp);
        return CLOSED_ERROR; 
    }
    //update is_closed, send broadcast signal to release all threads waiting on a cond var, they will all return CLOSED_ERROR after seeing the is_closed var is true
    channel -> is_closed = true; 
    if(channel->select_lockp != NULL){
        pthread_cond_signal(channel->select_condp);
        //channel->select_lockp = NULL; 
        //channel->select_condp = NULL; 
        //channel->any_channel_selectedp = NULL; 
    }
        pthread_cond_broadcast(&channel->is_empty_cond);
    pthread_cond_broadcast(&channel->is_full_cond);
    //now we are done with the data so unlock the mutex
    pthread_mutex_unlock(channel->lockp);
    return SUCCESS;
}

// Frees all the memory allocated to the channel
// The caller is responsible for calling channel_close and waiting for all threads to finish their tasks before calling channel_destroy
// Returns SUCCESS if destroy is successful,
// DESTROY_ERROR if channel_destroy is called on an open channel, and
// GEN_ERROR in any other error case
enum channel_status channel_destroy(channel_t* channel)
{
    pthread_mutex_lock(channel->lockp);
    if(!channel->is_closed)
    {
        pthread_mutex_unlock(channel->lockp);
        return DESTROY_ERROR; 
    }
    //space was allocated for the channel buffer var, channel_t var, two cond vars and the mutex var, so we must free them
    buffer_free(channel->buffer);
    pthread_cond_destroy(&channel->is_full_cond);
    pthread_cond_destroy(&channel->is_empty_cond);
    pthread_mutex_unlock(channel->lockp);
    pthread_mutex_destroy(channel->lockp);
    free(channel->lockp);
    free(channel);

    return SUCCESS;
}

enum channel_status channel_select_send_nonblocking(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
for(int i = 0; i < channel_count; i++)
    {
        select_t curr_select = channel_list[i];
        if(curr_select.channel->is_closed)
        {
            continue;
        }
        if(curr_select.dir == SEND)
        {
            if(channel_non_blocking_send(curr_select.channel, curr_select.data) == SUCCESS)
            {
                *selected_index = (size_t)i; 
                return SUCCESS;
            }
        }
        else
        {
            if(channel_non_blocking_receive(curr_select.channel, &(curr_select.data)) == SUCCESS)
            { 
                *selected_index = (size_t)i; 
                return SUCCESS;
            }
        }
    }
    return GEN_ERROR; 
}

enum channel_status set_channel_select_vars_to_null(select_t* channel_list, size_t channel_count)
{
    for(int i = 0; i<channel_count; i++)
    {
        channel_t* curr_channel = channel_list[i].channel; 
        if(curr_channel->is_closed)
        {
            continue;
        }
        pthread_mutex_lock(curr_channel->lockp);
        curr_channel->select_lockp = NULL; 
        curr_channel->select_condp = NULL; 
        curr_channel->any_channel_selectedp = NULL; 
        pthread_mutex_unlock(curr_channel->lockp);
    }
    return SUCCESS;
}

// Takes an array of channels (channel_list) of type select_t and the array length (channel_count) as inputs
// This API iterates over the provided list and finds the set of possible channels which can be used to invoke the required operation (send or receive) specified in select_t
// If multiple options are available, it selects the first option and performs its corresponding action
// If no channel is available, the call is blocked and waits till it finds a channel which supports its required operation
// Once an operation has been successfully performed, select should set selected_index to the index of the channel that performed the operation and then return SUCCESS
// In the event that a channel is closed or encounters any error, the error should be propagated and returned through select
// Additionally, selected_index is set to the index of the channel that generated the error
enum channel_status channel_select(select_t* channel_list, size_t channel_count, size_t* selected_index)
{
    //perform non blocking calls for all select_t in channel_list
    if(channel_select_send_nonblocking(channel_list, channel_count, selected_index) == SUCCESS)
    {
        return SUCCESS;
    }
    //create mutex and cond var for this select call, assign it to all channels in list
    pthread_mutex_t select_lock; 
    pthread_mutex_init(&select_lock, NULL); 
    pthread_cond_t select_cond;
    pthread_cond_init(&select_cond, NULL); 
    bool any_channel_selected = false; 

    for(int i = 0; i<channel_count; i++)
    {
        channel_t* curr_channel = channel_list[i].channel; 
        if(curr_channel->is_closed)
        {
            continue;
        }
        pthread_mutex_lock(curr_channel->lockp);
        curr_channel->select_lockp = &select_lock; 
        curr_channel->select_condp = &select_cond; 
        curr_channel->any_channel_selectedp = &any_channel_selected; 
        pthread_mutex_unlock(curr_channel->lockp);
    }
    //NOTE: Two mutex, if any channel struct change lock channel lock, if any select studd lock select lock
    //wait for other threads to do send and recieve calls, they will signal select if it exists for that channel before it signals another channel to continue
    pthread_mutex_lock(&select_lock);
    while(!any_channel_selected)
    {
        pthread_cond_wait(&select_cond, &select_lock);
        pthread_mutex_unlock(&select_lock);
        if(channel_select_send_nonblocking(channel_list, channel_count, selected_index) == SUCCESS)
        {
            any_channel_selected = true; 
            set_channel_select_vars_to_null(channel_list, channel_count);
            pthread_mutex_destroy(&select_lock);
            pthread_cond_destroy(&select_cond);
            return SUCCESS;
        }
        pthread_mutex_lock(&select_lock);
    }
    pthread_mutex_unlock(&select_lock);
    //if we were able to perform the function, loop through and remove select vars from channel and uninit
    set_channel_select_vars_to_null(channel_list, channel_count);
    pthread_mutex_destroy(&select_lock);
    pthread_cond_destroy(&select_cond);
    return GEN_ERROR;
}
