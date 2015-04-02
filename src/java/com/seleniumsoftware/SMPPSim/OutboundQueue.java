/****************************************************************************
 * OutboundQueue.java
 *
 * Copyright (C) Selenium Software Ltd 2006
 *
 * This file is part of SMPPSim.
 *
 * SMPPSim is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * SMPPSim is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SMPPSim; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * @author martin@seleniumsoftware.com
 * http://www.woolleynet.com
 * http://www.seleniumsoftware.com
 * $Header: /var/cvsroot/SMPPSim2/src/java/com/seleniumsoftware/SMPPSim/OutboundQueue.java,v 1.10 2014/05/25 10:42:27 martin Exp $
 ****************************************************************************
*/
package com.seleniumsoftware.SMPPSim;

import com.seleniumsoftware.SMPPSim.exceptions.*;
import com.seleniumsoftware.SMPPSim.pdu.SubmitSM;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.util.*;

/**
 * @author Martin Woolley
 *
 * Queue of MessageState objects
 * 
 * Processed by the State Lifecycle Service
 */
public class OutboundQueue implements Runnable {

	private static Logger logger = Logger.getLogger("com.seleniumsoftware.smppsim");

	private Smsc smsc = Smsc.getInstance();

	private LifeCycleManager lcm = smsc.getLcm();

	private final Map<MessageState,MessageState> queue;
	private final Lock lock = new ReentrantLock();
	private final Condition isEmpty = lock.newCondition();
	private boolean disable = true;
	
	
	public OutboundQueue(int maxsize) {
		queue = new ConcurrentHashMap<MessageState,MessageState>(maxsize){
			final AtomicInteger size = new AtomicInteger();

			@Override
			public int size() {
				return size.get();
			}

			@Override
			public MessageState put(MessageState key, MessageState value) {
				MessageState ms = super.put(key, value);
				if(ms == null){
					size.incrementAndGet();
				}
				return ms;
			}

			@Override
			public MessageState putIfAbsent(MessageState key, MessageState value) {
				MessageState ms = super.putIfAbsent(key, value);
				if(ms == null){
					size.incrementAndGet();
				}
				return ms;
			}

			@Override
			public MessageState remove(Object key) {
				MessageState ms = super.remove(key);
				if(ms != null){
					size.decrementAndGet();
				}
				return ms;
			}

			@Override
			public boolean remove(Object key, Object value) {
				boolean rc =  super.remove(key, value);
				if(rc){
					size.decrementAndGet();
				}
				return rc;
			}

			@Override
			public void clear() {
				super.clear();
				size.set(0);
			}
		};
	}

	public void addMessageState(MessageState message) throws OutboundQueueFullException {
		if(disable){
			return;
		}
		logger.finest("OutboundQueue: adding object to queue<" + message.toString() + ">");
		if (queue.size() < smsc.getOutbound_queue_capacity()) {
			queue.put(message, message);
			logger.fine("Added object to OutboundQueue. Queue now contains " + queue.size() + " object(s)");
			if (message.isIntermediate_notification_requested()) {
				SubmitSM p = message.getPdu();
				// delivery_receipt requested
				logger.info("Intermediate notification requested");
				smsc.prepareDeliveryReceipt(p, message.getMessage_id(), message.getState(), 1, 1, message.getErr());
			}
			try{
				lock.lock();
				isEmpty.signalAll();
			}finally{
				lock.unlock();
			}
		} else
			throw new OutboundQueueFullException("Request to add to OutboundQueue rejected as to do so would exceed max size of "
					+ smsc.getOutbound_queue_capacity());
	}

	public void setResponseSent(MessageState m) throws MessageStateNotFoundException {
		if(disable){
			return;
		}
		m.setResponseSent(true);
		updateMessageState(m);
	}

	public MessageState getMessageState(MessageState m) throws MessageStateNotFoundException {
		if(disable){
			return m;
		}
		//		logger.info("getMessageState:"+m.keyToString());
		MessageState message = queue.get(m);
		//		logger.info("queue pos="+i);
		if (message != null) {
			return message;
		} else {
			throw new MessageStateNotFoundException();
		}
	}

	public void updateMessageState(MessageState newMs) throws MessageStateNotFoundException {
		if(disable){
			return;
		}
		
		MessageState message = queue.get(newMs);
		if (message != null) {
			queue.put(newMs, newMs);
		} else {
			throw new MessageStateNotFoundException();
		}
	}

	public MessageState queryMessageState(String message_id, int ton, int npi, String addr) throws MessageStateNotFoundException {
		
		MessageState m = new MessageState();
		m.setMessage_id(message_id);
		m.setSource_addr_ton(ton);
		m.setSource_addr_npi(npi);
		m.setSource_addr(addr);
		return getMessageState(m);
	}

	public void removeMessageState(MessageState m) {
		if(disable){
			return;
		}
		MessageState message= queue.remove(m);
		if (message != null) {
			//logger.fine("Removed object from OutboundQueue. Queue now contains " + queue.size() + " object(s)");
		} else {
			logger.warning("Attempt to remove non-existent object from OutboundQueue: " + m.toString());
		}
	}

	public Object[] getAllMessageStates() {
		return (Object[]) queue.keySet().toArray();
	}

	protected boolean isEmpty() {
		return queue.isEmpty();
	}

	public void run() {
		// This code processes the contents of the OutboundQueue
		// Each object in the queue is a MessageState object and the purpose of
		// the OutboundQueue is to support QUERY_SM and REPLACE_SM operations.
		//
		// This code is termed the Lifecycle Manager Service.

		logger.info("Starting Lifecycle Service (OutboundQueue)");
		do // process queue
		{
			processQueue();
		} while (smsc.isRunning());
		logger.info("Lifecycle Service (OutboundQueue) is exiting");

	}

	private void processQueue() {
		Object[] messages = null;
		int count;
		long start;
		long finish;
		long duration;
		long sleeptime;
			while (smsc.isRunning()) {
				try {
					try{
						lock.lock();
						if(isEmpty()){
							logger.info("Lifecycle Service: OutboundQueue is empty  - waiting");
							isEmpty.await();
						}
					}finally{
						lock.unlock();
					}
				} catch (InterruptedException e) {
					if (smsc.isRunning()) {
						logger.log(Level.WARNING, "Exception in OutboundQueue: " + e.getMessage(), e);
					}
				}
			}
			start = System.currentTimeMillis();
			// Can allow other threads to compete for lock now since working on array from this point
		if (!smsc.isRunning()) {
			return;
		}
		count = messages.length;
		logger.info("Assessing state of " + count + " messages in the OutboundQueue");
		for (MessageState m: queue.keySet()) {
			if (!m.responseSent()) {
				//logger.finest("Response not yet sent so ignoring this MessageState object for now");
				continue;
			}
			if (lcm.messageShouldBeDiscarded(m)) {
			//	logger.finest("Disarding OutboundQueue message " + m.toString());
				removeMessageState(m);
			} else {
				byte currentState = m.getState();
				m = lcm.setState(m);
			}
		}
		finish = System.currentTimeMillis();
		duration = finish - start;
		sleeptime = SMPPSim.getMessageStateCheckFrequency() - duration;
		if (sleeptime > 0) {
			try {
				logger.finest("Lifecycle Service sleeping for " + sleeptime + " milliseconds");
				Thread.sleep(sleeptime);
			} catch (InterruptedException e) {
			}
		} else {
			logger
					.warning("It's taking longer to process the OutboundQueue than MESSAGE_STATE_CHECK_FREQUENCY milliseconds. Recommend this value is increased");
		}
	}

	public int size() {
		return queue.size();
	}
}
