// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.optimizer.operator;

import org.apache.doris.optimizer.base.EnforceOrderProperty;
import org.apache.doris.optimizer.base.EnforceProperty;
import org.apache.doris.optimizer.base.OptOrderSpec;
import org.apache.doris.optimizer.base.OptPhysicalProperty;
import org.apache.doris.optimizer.base.OptProperty;
import org.apache.doris.optimizer.base.RequiredPhysicalProperty;

public abstract class OptPhysical extends OptOperator {

    protected OptPhysical(OptOperatorType type) {
        super(type);
    }

    @Override
    public boolean isPhysical() { return true; }
    @Override
    public OptProperty createProperty() {
        return new OptPhysicalProperty();
    }

    public RequiredPhysicalProperty getPhysicalProperty() { return null; }

    //------------------------------------------------------------------------
    // Used to compute required property for children
    //------------------------------------------------------------------------
    // get required sort order of n-th child
    public abstract EnforceOrderProperty getChildReqdOrder(
            OptExpressionHandle handle,
            EnforceOrderProperty reqdOrder,
            int childIndex);

    //------------------------------------------------------------------------
    // Used to get operator's derived property
    //------------------------------------------------------------------------
    // get derived order property for this operator
    public abstract OptOrderSpec getOrderSpec(OptExpressionHandle exprHandle);

    //------------------------------------------------------------------------
    // Used to get enforce type for this operator
    //------------------------------------------------------------------------
    // get enforce type for sort order
    public abstract EnforceProperty.EnforceType getOrderEnforceType(
            OptExpressionHandle exprHandle, EnforceOrderProperty enforceOrder);

    protected  OptOrderSpec getOrderSpecPassThrough(OptExpressionHandle exprHandle) {
        return exprHandle.getChildPhysicalProperty(0).getOrderSpec();
    }
}