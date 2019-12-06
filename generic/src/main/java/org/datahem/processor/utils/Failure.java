  package org.datahem.processor.utils;

/*-
 * ========================LICENSE_START=================================
 * DataHem
 * %%
 * Copyright (C) 2018 - 2019 MatHem Sverige AB
 * %%
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License along with this program. If not, see <https://www.gnu.org/licenses/>.
 * =========================LICENSE_END==================================
 */
  
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import java.util.ArrayList;
import java.util.List;

  public class Failure{
        private String target;
        private String message;
        private String error;
        private String errorType;

    public Failure(String target, String message, String error, String errorType){
        this.target = target;
        this.message = message;
        this.error = error;
        this.errorType = errorType;
    }

    public String getTarget(){return target;}
    public String getMessage(){return message;}
    public String getError(){return error;}
    public String getErrorType(){return errorType;}

    public TableRow getAsTableRow(){
        TableRow outputRow = new TableRow();
        outputRow.set("Target", this.getTarget());
        outputRow.set("Message", this.getMessage());
        outputRow.set("Error", this.getError());
        outputRow.set("ErrorType", this.getError());
        return outputRow;
    }

    public static TableSchema getTableSchema(){
        TableSchema errorSchema = new TableSchema();
        List<TableFieldSchema> errorSchemaFields = new ArrayList<TableFieldSchema>();
        errorSchemaFields.add(new TableFieldSchema().setName("Target").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("Message").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("Error").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchemaFields.add(new TableFieldSchema().setName("ErrorType").setType("STRING").setMode("NULLABLE").setDescription(""));
        errorSchema.setFields(errorSchemaFields);
        return errorSchema;
    }
        

  }