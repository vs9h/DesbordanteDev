// Strutovsky, 29.08.2019

#include "TaneX.h"
#include "../model/ColumnLayoutRelationData.h"
#include "../model/RelationalSchema.h"
#include "../model/ColumnData.h"
#include "../model/ColumnCombination.h"
#include "../util/LatticeVertex.h"
#include "../util/LatticeLevel.h"
#include <iostream>
#include <iomanip>
#include <chrono>
#include <list>
//#define log(x) cout << (x) << endl;

using boost::dynamic_bitset, std::make_shared, std::shared_ptr, std::cout, std::endl, std::setw, std::vector, std::list;

double Tane::calculateZeroAryFdError(shared_ptr<ColumnData> rhs, shared_ptr<ColumnLayoutRelationData> relationData) {
    return 1 - rhs->getPositionListIndex()->getNep() / (double)relationData->getNumTuplePairs();
}

double Tane::calculateFdError(shared_ptr<PositionListIndex> lhsPli, shared_ptr<PositionListIndex> jointPli, shared_ptr<ColumnLayoutRelationData> relationData) {
    return (double) (lhsPli->getNep() - jointPli->getNep()) / (double) relationData->getNumTuplePairs();
}


double Tane::calculateUccError(shared_ptr<PositionListIndex> pli, shared_ptr<ColumnLayoutRelationData> relationData) {
    return pli->getNep() / (double) relationData->getNumTuplePairs();
}

void Tane::registerFD(Vertical& lhs, shared_ptr<Column> rhs, double error) {
    dynamic_bitset<> lhs_bitset = lhs.getColumnIndices();
    cout << "Discovered FD: ";
    for (int i = lhs_bitset.find_first(); i != -1; i = lhs_bitset.find_next(i)) {
        cout << i << " ";
    }
    cout << "-> " << rhs->getIndex() << " - error equals " << error << endl;
}

void Tane::registerFD(shared_ptr<Vertical> lhs, shared_ptr<Column> rhs, double error) {
    dynamic_bitset<> lhs_bitset = lhs->getColumnIndices();
    cout << "Discovered FD: ";
    for (int i = lhs_bitset.find_first(); i != -1; i = lhs_bitset.find_next(i)) {
        cout << i << " ";
    }
    cout << "-> " << rhs->getIndex() << " - error equals " << error << endl;
}

void Tane::registerUCC(Vertical& key, double error) {
    dynamic_bitset<> key_bitset = key.getColumnIndices();
    cout << "Discovered UCC: ";
    for (int i = key_bitset.find_first(); i != -1; i = key_bitset.find_next(i)) {
        cout << i << " ";
    }
    cout << "- error equals " << error << endl;
}


void Tane::execute() {

  shared_ptr<ColumnLayoutRelationData> relation = ColumnLayoutRelationData::createFrom(inputGenerator, true);
  //TODO: this row won't work at all, so dig into this topic (because of inputGen initialization absence) - FIXED

  shared_ptr<RelationalSchema> schema = relation->getSchema();
  cout << schema->getName() << " has " << relation->getNumColumns() << " columns, "
       << relation->getNumRows() << " rows, and a maximum NIP of " << setw(2)
       << relation->getMaximumNip() << "." << endl;

  for (auto column : schema->getColumns()) {
    shared_ptr<ColumnData> columnData = relation->getColumnData(column->getIndex());
    double avgPartners = columnData->getPositionListIndex()->getNep() * 2.0 / relation->getNumRows();
    cout << "* " << column->toString() << ": every tuple has " << setw(2)
         << avgPartners << " partners on average." << endl;
  }

  //Initialize level 0
  vector<shared_ptr<LatticeLevel>> levels;
  shared_ptr<LatticeLevel> level0 = make_shared<LatticeLevel>(0);
  shared_ptr<LatticeVertex> emptyVertex = make_shared<LatticeVertex>(*(schema->emptyVertical)); //TODO: resolve design conflict (func accepts ref, gets pointer)
  level0->add(emptyVertex);
  levels.push_back(level0);

  //Initialize level1
  dynamic_bitset<> zeroaryFdRhs(schema->getNumColumns());
  shared_ptr<LatticeLevel> level1 = make_shared<LatticeLevel>(1);
  for (auto column : schema->getColumns()) {
      //for each attribute set vertex
    shared_ptr<ColumnData> columnData = relation->getColumnData(column->getIndex());
    shared_ptr<LatticeVertex> vertex = make_shared<LatticeVertex>(LatticeVertex(static_cast<Vertical>(*column)));          //Column->Vertical--copy_constr->LV?
    //TODO: ^ is required to be constructed from column - check column in the next for - perhaps implement LV(shared_ptr<Vertical>), this ptr points to Column?
    vertex->addRhsCandidates(schema->getColumns());                 //Had to remake addRhsCandidates to accept Columns
    vertex->getParents().push_back(emptyVertex);
    vertex->setKeyCandidate(true);
    vertex->setPositionListIndex(columnData->getPositionListIndex());
    level1->add(vertex);

    //check FDs: 0->A
    double fdError = calculateZeroAryFdError(columnData, relation);;  //TODO: error
    if (fdError <= maxFdError) {  //TODO: max_error
      zeroaryFdRhs.set(column->getIndex());
      //TODO: registerFd
      registerFD(schema->emptyVertical, column, fdError);
      //cout << "AAAA" << endl;
      vertex->getRhsCandidates().set(column->getIndex(), false);
      if (fdError == 0) {
        vertex->getRhsCandidates().clear();
      }
    }
  }

  for (auto [key_map, vertex] : level1->getVertices()) {
      //Originally column is of type Column
    Vertical& column = vertex->getVertical();               //TODO: perhaps store V as pointer in LV??
    //TODO: tricky conversion: it looks like we originally constructed  vertex using columns - deprecated for now,
    //so level1 has pointer to Vertices that actually point to columns, therefore we should be able to cast them somehow back to *columns
    //would be possible if vertical had been an abstract class - I suppose, it is possible by using downcast with static_pointer_cast
    vertex->getRhsCandidates() &= ~zeroaryFdRhs;  //~ returns flipped copy <- removed already discovered zeroary FDs

    shared_ptr<ColumnData> columnData = relation->getColumnData(column.getColumnIndices().find_first());    //KOCTbIJlN!!! - this column has only one index
    double uccError = calculateUccError(columnData->getPositionListIndex(), relation);;  //TODO: uccErrorMeasure
    if (uccError <= maxUccError) {
      //TODO: do something with discovered UCC
      registerUCC(column, uccError);
      vertex->setKeyCandidate(false);
      if (uccError == 0) {
        for (unsigned long rhsIndex = vertex->getRhsCandidates().find_first();
             rhsIndex < vertex->getRhsCandidates().size();              //Possible to do it faster?
             rhsIndex = vertex->getRhsCandidates().find_next(rhsIndex + 1)){
          if (rhsIndex != column.getColumnIndices().find_first()){                          //KOSTYL'!!
              registerFD(column, schema->getColumn(rhsIndex), 0);
            //TODO: do smth with registered FD
          }
        }
        vertex->getRhsCandidates() &= column.getColumnIndices();
        //set vertex invalid if we seek for exact dependencies
        if (maxFdError == 0 && maxUccError == 0) {     //TODO: error constants
          vertex->setInvalid(true);
        }
      }
    }
  }
  levels.push_back(level1);

  //TODO: configuration.maxArity
  for (int arity = 2; arity <= maxArity || maxArity <= 0; arity++) {
      //Generate next level - CHECK if the method itself is correct
    auto startTime = std::chrono::system_clock::now();
    LatticeLevel::clearLevelsBelow(levels, arity - 1);
    LatticeLevel::generateNextLevel(levels);
    std::chrono::duration<double> elapsed_milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - startTime);
    aprioriMillis += elapsed_milliseconds.count();

    shared_ptr<LatticeLevel> level = levels[arity];   //TODO: careful, should write arity-1? - No, its OK
    cout << "Checking " << level->getVertices().size() << " " << arity << "-ary lattice vertices." << endl;
    if (level->getVertices().empty()) {
      break;
    }

    for (auto [key_map, xaVertex] : level->getVertices()) {
      if (xaVertex->getIsInvalid()){
        continue;
      }

      //TODO: check the following conversion:
      Vertical& xa = xaVertex->getVertical();       //- previously it was a ColumnCombination
      //Calculate XA PLI
      if (xaVertex->getPositionListIndex() == nullptr) {
          //TODO: Can parents be implemented as std::pair??
        shared_ptr<PositionListIndex> parentPLI1 = xaVertex->getParents()[0]->getPositionListIndex();   //OK??
        shared_ptr<PositionListIndex> parentPLI2 = xaVertex->getParents()[1]->getPositionListIndex();
        xaVertex->setPositionListIndex(parentPLI1->intersect(parentPLI2));
      }

      dynamic_bitset xaIndices = xa.getColumnIndices();
      dynamic_bitset aCandidates = xaVertex->getRhsCandidates();

      for (const auto& xVertex : xaVertex->getParents()) {
        Vertical& lhs = xVertex->getVertical();
        //TODO: faster to return shared_ptr to a lhs and use it afterwards or to return a & and call getVertical everytime?

        // Find index of A in XA. If a is not a candidate, continue. TODO: possible to do it easier??
        //like "aIndex = xaIndices - xIndices;"
        int aIndex = xaIndices.find_first();
        dynamic_bitset xIndices = lhs.getColumnIndices();
        while (aIndex >= 0 && xIndices[aIndex]) {
          aIndex = xaIndices.find_next(aIndex);
        }
        if (!aCandidates[aIndex]) {
          continue;
        }

        // Check X -> A
        double error = calculateFdError(
                  xVertex->getPositionListIndex(),
                  xaVertex->getPositionListIndex(),
                  relation);
        if (error <= maxFdError) {  //TODO: and again...
          shared_ptr<Column> rhs = schema->getColumns()[aIndex];
          //TODO: register FD
          registerFD(lhs, rhs, error);
          xaVertex->getRhsCandidates().set(rhs->getIndex(), false);
          if (error == 0) {
            xaVertex->getRhsCandidates() &= lhs.getColumnIndices(); //Can't figure out what it is for
          }
        }
      }
    }

    //Prune
    list<shared_ptr<LatticeVertex>> keyVertices;
    for (auto [map_key, vertex] : level->getVertices()) {
      Vertical& columns = vertex->getVertical();            //Originally it's a ColumnCombination

      if (vertex->getIsKeyCandidate()) {
        double uccError = calculateUccError(vertex->getPositionListIndex(), relation); //TODO
        if (uccError <= maxUccError) {       //If a key candidate is an approx UCC
          //TODO: do smth with UCC

          registerUCC(columns, uccError);
          vertex->setKeyCandidate(false);
          if (uccError == 0) {
              // Look at 185 for this cycle description in a nutshell
            for (unsigned long rhsIndex = vertex->getRhsCandidates().find_first();
                 rhsIndex < vertex->getRhsCandidates().size();      //TODO: check that it's a correct way to traverse bitset
                 rhsIndex = vertex->getRhsCandidates().find_next(rhsIndex + 1)) {
              Vertical rhs = *(std::static_pointer_cast<Vertical>(schema->getColumn((int)rhsIndex)));        //TODO: KOSTYL' - upcast - should be OK
              if (!columns.contains(rhs)) {         //TODO: again this conversion (P.S. was)
                bool isRhsCandidate = true;
                for (const auto& column : columns.getColumns()) { //TODO: implement getColumns - DONE, but it returns v<Vertical>
                  Vertical sibling = columns.without(*std::static_pointer_cast<Vertical>(column)).Union(rhs);   //check types carfully
                  shared_ptr<LatticeVertex> siblingVertex = level->getLatticeVertex(sibling.getColumnIndices());
                  if (siblingVertex == nullptr || !siblingVertex->getRhsCandidates()[rhs.getColumnIndices().find_first()]) { //TODO: KOSTYL'
                    isRhsCandidate = false;
                    break;
                  }
                  // for each outer rhs: if there is a sibling s.t. it doesn't have this rhs, there is no FD: vertex->rhs
                }
                //Found fd: vertex->rhs => register it
                if (isRhsCandidate){
                  //TODO: smth with FD
                  registerFD(columns, schema->getColumn(rhsIndex), 0);
                }
              }
              keyVertices.push_back(vertex);
            }
          }
        }
      }
      //if we seek for exact FDs then setInvalid
      if (maxFdError == 0 && maxUccError == 0){
        for (auto keyVertex : keyVertices){
          keyVertex->getRhsCandidates() &= keyVertex->getVertical().getColumnIndices();
          keyVertex->setInvalid(true);
        }
      }
    }

    //TODO: printProfilingData
  }
}
